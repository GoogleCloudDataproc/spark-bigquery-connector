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

import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.storage.v1beta1.Storage.{ReadRowsRequest, ReadRowsResponse, StreamPosition}
import com.google.cloud.bigquery.storage.v1beta1.{BigQueryStorageClient, Storage}
import com.google.cloud.spark.bigquery.{SchemaConverters, SparkBigQueryOptions}
import com.google.protobuf.ByteString
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._

class BigQueryRDD(sc: SparkContext,
    parts: Array[Partition],
    sessionId: String,
    columnsInOrder: Seq[String],
    rawAvroSchema: String,
    bqSchema: Schema,
    options: SparkBigQueryOptions,
    getClient: SparkBigQueryOptions => BigQueryStorageClient)
    extends RDD[InternalRow](sc, Nil) {

  @transient private lazy val avroSchema = new AvroSchema.Parser().parse(rawAvroSchema)
  private lazy val converter = SchemaConverters.createRowConverter(bqSchema, columnsInOrder) _

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bqPartition = split.asInstanceOf[BigQueryPartition]
    val bqStream = Storage.Stream.newBuilder().setName(bqPartition.stream).build()
    val request = ReadRowsRequest.newBuilder()
      .setReadPosition(StreamPosition.newBuilder().setStream(bqStream)).build()

    val client = getClient(options)
    // Taken from FileScanRDD
    context.addTaskCompletionListener(_ => client.close)

    try {
      // TODO(pmkc): unwrap RuntimeExceptions from iterator
      val it = client.readRowsCallable().call(request).iterator.asScala.flatMap(toRows)
      return new InterruptibleIterator(context, it)
    } catch {
      case e: Exception =>
        client.close()
        throw e
    }
  }

  def toRows(response: ReadRowsResponse): Iterator[InternalRow] = new AvroBinaryIterator(
    avroSchema,
    response.getAvroRows.getSerializedBinaryRows).map(converter)

  override protected def getPartitions: Array[Partition] = parts

  class AvroBinaryIterator(schema: AvroSchema, bytes: ByteString) extends Iterator[GenericRecord] {
    // TODO(pclay): replace nulls with reusable objects
    val reader = new GenericDatumReader[GenericRecord](schema)
    val in: BinaryDecoder = new DecoderFactory().binaryDecoder(bytes.toByteArray, null)

    override def hasNext: Boolean = !in.isEnd

    override def next(): GenericRecord = reader.read(null, in)
  }
}

case class BigQueryPartition(stream: String, index: Int) extends Partition

object BigQueryRDD {
  def scanTable(sqlContext: SQLContext,
      parts: Array[Partition],
      sessionId: String,
      avroSchema: String,
      bqSchema: Schema,
      columnsInOrder: Seq[String],
      options: SparkBigQueryOptions,
      getClient: SparkBigQueryOptions => BigQueryStorageClient): BigQueryRDD = {
    new BigQueryRDD(sqlContext.sparkContext,
      parts,
      sessionId,
      columnsInOrder: Seq[String],
      avroSchema,
      bqSchema,
      options,
      getClient)
  }
}
