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

import java.io.{BufferedOutputStream, ByteArrayInputStream, FileOutputStream, InputStream}

import com.google.cloud.bigquery.Schema
import com.google.cloud.spark.bigquery.converters.ArrowSchemaConverter
import com.google.protobuf.ByteString
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import collection.JavaConverters._

class ArrowBinaryIterator(columnsInOrder: Seq[String],
                          schema: ByteString,
                          bytes: ByteString) extends Iterator[InternalRow] {

  val allocator = ArrowUtils.rootAllocator.newChildAllocator("ArrowBinaryIterator",
    0, Long.MaxValue)

  val byteStringWithSchema = schema.concat(bytes)

  val arrowStreamReader = new ArrowStreamReader(
    new ByteArrayInputStream(byteStringWithSchema.toByteArray).asInstanceOf[InputStream]
    , allocator)

  val arrowReaderIterator = new ArrowReaderIterator(arrowStreamReader)
  val iterator = arrowReaderIterator.flatMap(root => toArrowRows(root, columnsInOrder))

  private def toArrowRows(root: VectorSchemaRoot, namesInOrder: Seq[String]):
  Iterator[InternalRow] = {
    val vectors = namesInOrder.map(root.getVector)

    val columns = vectors.map { vector =>
      try {
        new ArrowSchemaConverter(vector).asInstanceOf[ColumnVector]
      } catch {
        case e : UnsupportedOperationException =>
          throw new Exception(vector.getClass.getName, e)
      }
    }.toArray

    val batch = new ColumnarBatch(columns)
    batch.setNumRows(root.getRowCount)
    batch.rowIterator().asScala
  }

  class ArrowReaderIterator(reader: ArrowReader) extends Iterator[VectorSchemaRoot] {
    var closed = false
    var current: VectorSchemaRoot = null

    def ensureClosed(): Unit = {
      if (!closed) {
        reader.close()
        closed = true
      }
    }

    override def hasNext: Boolean = {
      if (current != null) {
        return true
      }
      val res = reader.loadNextBatch()
      if (res) {
        current = reader.getVectorSchemaRoot
      } else {
        ensureClosed()
      }
      res
    }

    override def next(): VectorSchemaRoot = {
      assert(current != null)
      val res = current
      current = null
      res
    }
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): InternalRow = iterator.next()
}
