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

import java.io.{ByteArrayInputStream, SequenceInputStream}

import com.google.protobuf.ByteString
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import collection.JavaConverters._

/**
 * An iterator for scanning over rows serialized in Arrow format
 * @param columnsInOrder Sequence of columns in the schema
 * @param schema Schema in arrow format
 * @param rowsInBytes Rows serialized in binary format for Arrow
 */
class ArrowBinaryIterator(columnsInOrder: Seq[String],
                          schema: ByteString,
                          rowsInBytes: ByteString) extends Iterator[InternalRow] {

  val allocator = ArrowBinaryIterator.rootAllocator.newChildAllocator("ArrowBinaryIterator",
    0, ArrowBinaryIterator.maxAllocation)

  val bytesWithSchemaStream = new SequenceInputStream(
    new ByteArrayInputStream(schema.toByteArray),
    new ByteArrayInputStream(rowsInBytes.toByteArray))

  val arrowStreamReader = new ArrowStreamReader(bytesWithSchemaStream, allocator)

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

object ArrowBinaryIterator {
  // max allocation value for the allocator
  var maxAllocation = Long.MaxValue
  val rootAllocator = new RootAllocator(maxAllocation)
}
