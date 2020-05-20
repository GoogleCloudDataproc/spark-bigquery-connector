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
package com.google.cloud.spark.bigquery

import com.google.cloud.bigquery.Schema
import com.google.protobuf.ByteString
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConverters

/**
 * An iterator for scanning over rows serialized in Avro format
 * @param bqSchema Schema of underlying BigQuery source
 * @param columnsInOrder Sequence of columns in the schema
 * @param schema Schema in avro format
 * @param rowsInBytes Rows serialized in binary format for Avro
 */
class AvroBinaryIterator(bqSchema: Schema,
                         columnsInOrder: Seq[String],
                         schema: AvroSchema,
                         rowsInBytes: ByteString) extends Iterator[InternalRow] {

  val reader = new GenericDatumReader[GenericRecord](schema)
  val columnsInOrderList = JavaConverters.seqAsJavaListConverter(columnsInOrder).asJava
  val in: BinaryDecoder = new DecoderFactory().binaryDecoder(rowsInBytes.toByteArray, null)

  override def hasNext: Boolean = !in.isEnd

  override def next(): InternalRow = SchemaConverters.createRowConverter(bqSchema,
    columnsInOrderList, reader.read(null, in))
}
