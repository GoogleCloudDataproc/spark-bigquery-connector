/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.direct

import com.google.api.gax.rpc.ServerStreamingCallable
import com.google.cloud.bigquery.storage.v1beta1.Storage.{ReadRowsRequest, ReadRowsResponse, StreamPosition}
import com.google.cloud.bigquery.storage.v1beta1.{BigQueryStorageClient, Storage}
import com.google.cloud.bigquery.{BigQuery, Schema}
import com.google.cloud.spark.bigquery.{ArrowBinaryIterator, AvroBinaryIterator, BigQueryUtil, SchemaConverters, SparkBigQueryOptions}
import com.google.protobuf.ByteString
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.MutableList._

class BigQueryRDD(sc: SparkContext,
                  parts: Array[Partition],
                  sessionId: String,
                  columnsInOrder: Seq[String],
                  rawAvroSchema: String,
                  bqSchema: Schema,
                  options: SparkBigQueryOptions,
                  getClient: SparkBigQueryOptions => BigQueryStorageClient,
                  bigQueryClient: SparkBigQueryOptions => BigQuery)
  extends RDD[InternalRow](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bqPartition = split.asInstanceOf[BigQueryPartition]
    val bqStream = Storage.Stream.newBuilder().setName(bqPartition.stream).build()
    val request = ReadRowsRequest.newBuilder()
      .setReadPosition(StreamPosition.newBuilder().setStream(bqStream))

    val client = getClient(options)
    // Taken from FileScanRDD
    context.addTaskCompletionListener(ctx => {
      client.close
      ctx
    })

    val readRowResponses = ReadRowsHelper(
      ReadRowsClientWrapper(client), request, options.maxReadRowsRetries)
      .readRows()

    val it = AvroConverter(bqSchema, columnsInOrder, rawAvroSchema, readRowResponses).getIterator()

    new InterruptibleIterator(context, it)
  }

  override protected def getPartitions: Array[Partition] = parts
}

/**
 * A converter for transforming an iterator on ReadRowsResponse to an iterator
 * that converts each of these rows to Arrow Schema
 * @param columnsInOrder Ordered columns in the Big Query schema
 * @param rawArrowSchema Schema representation in arrow format
 * @param rowResponseIterator Iterator over rows read from big query
 */
case class ArrowConverter(columnsInOrder: Seq[String],
                          rawArrowSchema : ByteString,
                          rowResponseIterator : Iterator[ReadRowsResponse])
{
  def getIterator(): Iterator[InternalRow] = {
    rowResponseIterator.flatMap(readRowResponse =>
      new ArrowBinaryIterator(columnsInOrder,
        rawArrowSchema,
        readRowResponse.getArrowRecordBatch.getSerializedRecordBatch));
  }
}

/**
 * A converter for transforming an iterator on ReadRowsResponse to an iterator
 * that converts each of these rows to Avro Schema
 * @param bqSchema Schema of underlying big query source
 * @param columnsInOrder Ordered columns in the Big Query schema
 * @param rawAvroSchema Schema representation in Avro format
 * @param rowResponseIterator Iterator over rows read from big query
 */
case class AvroConverter (bqSchema: Schema,
                 columnsInOrder: Seq[String],
                 rawAvroSchema: String,
                 rowResponseIterator : Iterator[ReadRowsResponse])
{
  @transient private lazy val avroSchema = new AvroSchema.Parser().parse(rawAvroSchema)

  def getIterator(): Iterator[InternalRow] =
  {
    rowResponseIterator.flatMap(toRows)
  }

  def toRows(response: ReadRowsResponse): Iterator[InternalRow] = new AvroBinaryIterator(
    bqSchema,
    columnsInOrder,
    avroSchema,
    response.getAvroRows.getSerializedBinaryRows)
}

case class BigQueryPartition(stream: String, index: Int) extends Partition

trait ReadRowsClient extends AutoCloseable {
  def readRowsCallable: ServerStreamingCallable[ReadRowsRequest, ReadRowsResponse]
}

case class ReadRowsClientWrapper(client: BigQueryStorageClient)
  extends ReadRowsClient {

  override def readRowsCallable: ServerStreamingCallable[ReadRowsRequest, ReadRowsResponse] =
    client.readRowsCallable

  override def close: Unit = client.close
}

case class ReadRowsHelper(
                           client: ReadRowsClient,
                           request: ReadRowsRequest.Builder,
                           maxReadRowsRetries: Int
                         ) extends Logging {
  def readRows(): Iterator[ReadRowsResponse] = {
    val readPosition = request.getReadPositionBuilder
    val readRowResponses = new mutable.MutableList[ReadRowsResponse]
    var readRowsCount: Long = 0
    var retries: Int = 0
    var serverResponses = fetchResponses(request)
    while (serverResponses.hasNext) {
      try {
        val response = serverResponses.next
        readRowsCount += response.getRowCount
        readRowResponses += response
        logInfo(s"read ${response.getSerializedSize} bytes")
      } catch {
        case e: Exception =>
          // if relevant, retry the read, from the last read position
          if (BigQueryUtil.isRetryable(e) && retries < maxReadRowsRetries) {
            serverResponses = fetchResponses(request.setReadPosition(
              readPosition.setOffset(readRowsCount)))
            retries += 1
          } else {
            client.close
            throw e
          }
      }
    }
    readRowResponses.iterator
  }

  // In order to enable testing
  protected def fetchResponses(readRowsRequest: ReadRowsRequest.Builder): java.util.Iterator[
    ReadRowsResponse] =
    client.readRowsCallable
    .call(readRowsRequest.build)
    .iterator

}

object BigQueryRDD {
  def scanTable(sqlContext: SQLContext,
                parts: Array[Partition],
                sessionId: String,
                avroSchema: String,
                bqSchema: Schema,
                columnsInOrder: Seq[String],
                options: SparkBigQueryOptions,
                getClient: SparkBigQueryOptions => BigQueryStorageClient,
                bigQueryClient: SparkBigQueryOptions => BigQuery): BigQueryRDD = {
    new BigQueryRDD(sqlContext.sparkContext,
      parts,
      sessionId,
      columnsInOrder: Seq[String],
      avroSchema,
      bqSchema,
      options,
      getClient,
      bigQueryClient)
  }
}
