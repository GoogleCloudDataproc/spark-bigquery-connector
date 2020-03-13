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

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.LegacySQLTypeName.{BOOLEAN, BYTES, DATE, DATETIME, FLOAT, INTEGER, NUMERIC, RECORD, STRING, TIME, TIMESTAMP}
import com.google.cloud.bigquery.{Field, Schema}
import com.google.common.io.ByteStreams.toByteArray
import com.google.protobuf.ByteString
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.sql.types.{ArrayType, BinaryType, StructType}

class SchemaIteratorSuite extends org.scalatest.FunSuite {

  test("compare arrow and avro results") {

    val numFields = 12
    val avroByteString = ByteString.copyFrom(
      toByteArray(getClass.getResourceAsStream("/avrobytearray")))
    val arrowByteString = ByteString.copyFrom(
      toByteArray(getClass.getResourceAsStream("/arrowbytearray")))

    val avroSchema = new AvroSchema.Parser().parse(getClass.getResourceAsStream("/avroschema.json"))
    val arrowSchema = ByteString.copyFrom(toByteArray(getClass.getResourceAsStream("/arrowschema")))
    val columnsInOrder = Seq("int_req", "int_null", "bl", "str", "day", "ts", "dt", "tm", "binary",
      "float", "nums", "int_arr", "int_struct_arr")

    val bqSchema = Schema.of(
      Field.newBuilder("int_req", INTEGER).setMode(Mode.REQUIRED).build(),
      Field.of("int_null", INTEGER),
      Field.of("bl", BOOLEAN),
      Field.of("str", STRING),
      Field.of("day", DATE),
      Field.of("ts", TIMESTAMP),
      Field.of("dt", DATETIME),
      Field.of("tm", TIME),
      Field.of("binary", BYTES),
      Field.of("float", FLOAT),
      Field.of("nums", RECORD,
        Field.of("min", NUMERIC),
        Field.of("max", NUMERIC),
        Field.of("pi", NUMERIC),
        Field.of("big_pi", NUMERIC)),
      Field.newBuilder("int_arr", INTEGER).setMode(Mode.REPEATED).build(),
      Field.newBuilder("int_struct_arr", RECORD,
        Field.of("i", INTEGER)).setMode(Mode.REPEATED).build())

    val schemaFields = SchemaConverters.toSpark(bqSchema).fields

    val arrowSparkRow = new ArrowBinaryIterator(columnsInOrder, arrowSchema, arrowByteString)
      .next()

    val avroSparkRow = new AvroBinaryIterator(bqSchema,
      columnsInOrder, avroSchema, avroByteString).next()

    for (col <- 0 to numFields)
    {
        if (arrowSparkRow.isNullAt(col))
        {
            assert(avroSparkRow.isNullAt(col))
        }
        else
        {
            val schemaFieldDataType = schemaFields.apply(col).dataType

            if (schemaFieldDataType == BinaryType)
            {
                avroSparkRow.getBinary(col).equals(arrowSparkRow.getBinary(col))
            }
            else
            if (schemaFieldDataType == ArrayType)
            {
              assert(avroSparkRow.getArray(col).equals(arrowSparkRow.getArray(col)))
            }
            else
            if (schemaFieldDataType == StructType)
            {
              assert(avroSparkRow.getStruct(col, 4).equals(arrowSparkRow.getStruct(col, 4)))
            }
            else
            {
              assert(avroSparkRow.get(col, schemaFieldDataType).equals(
                arrowSparkRow.get(col, schemaFieldDataType)))
            }
        }
    }
  }
}
