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
import com.google.cloud.bigquery.connector.common.{BigQueryClientFactory, ReadRowsHelper}
import com.google.cloud.bigquery.storage.v1.{DataFormat, ReadRowsRequest, ReadRowsResponse, ReadSession}
import com.google.cloud.spark.bigquery.{ArrowBinaryIterator, AvroBinaryIterator, SparkBigQueryConfig}
import com.google.protobuf.ByteString
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.util.Optional
import scala.collection.JavaConverters._

class BigQueryRDD(sc: SparkContext,
                  parts: Array[Partition],
                  session: ReadSession,
                  columnsInOrder: Seq[String],
                  bqSchema: Schema,
                  options: SparkBigQueryConfig,
                  bigQueryClientFactory: BigQueryClientFactory)
  extends RDD[InternalRow](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bqPartition = split.asInstanceOf[BigQueryPartition]
    val request = ReadRowsRequest.newBuilder().setReadStream(bqPartition.stream)
    val readRowsHelper = new ReadRowsHelper(bigQueryClientFactory, request, options.toReadSessionCreatorConfig.toReadRowsHelperOptions);

    val readRowResponses = readRowsHelper.readRows().asScala

    val it = if (options.getReadDataFormat.equals(DataFormat.AVRO)) {
      AvroConverter(bqSchema,
        columnsInOrder,
        session.getAvroSchema.getSchema,
        readRowResponses,
        options.getSchema).getIterator()
    }
    else {
      ArrowConverter(columnsInOrder,
        session.getArrowSchema.getSerializedSchema,
        readRowResponses,
        options.getSchema).getIterator()
    }

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
                          rowResponseIterator : Iterator[ReadRowsResponse],
                          userProvidedSchema: Optional[StructType])
{
  def getIterator(): Iterator[InternalRow] = {
    rowResponseIterator.flatMap(readRowResponse =>
      new ArrowBinaryIterator(columnsInOrder.asJava,
        rawArrowSchema,
        readRowResponse.getArrowRecordBatch.getSerializedRecordBatch,
        userProvidedSchema).asScala);
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
                 rowResponseIterator : Iterator[ReadRowsResponse],
                 userProvidedSchema: Optional[StructType])
{
  @transient private lazy val avroSchema = new AvroSchema.Parser().parse(rawAvroSchema)

  def getIterator(): Iterator[InternalRow] =
  {
    rowResponseIterator.flatMap(toRows)
  }

  def toRows(response: ReadRowsResponse): Iterator[InternalRow] = new AvroBinaryIterator(
    bqSchema,
    columnsInOrder.asJava,
    avroSchema,
    response.getAvroRows.getSerializedBinaryRows,
    userProvidedSchema).asScala
}

case class BigQueryPartition(stream: String, index: Int) extends Partition

object BigQueryRDD {
  def scanTable(sqlContext: SQLContext,
                parts: Array[Partition],
                session: ReadSession,
                bqSchema: Schema,
                columnsInOrder: Seq[String],
                options: SparkBigQueryConfig,
                bigQueryReadClientFactory: BigQueryClientFactory): BigQueryRDD = {
    new BigQueryRDD(sqlContext.sparkContext,
      parts,
      session,
      columnsInOrder: Seq[String],
      bqSchema,
      options,
      bigQueryReadClientFactory)
  }
}
