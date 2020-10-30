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
// package com.google.cloud.spark.bigquery;
//
// import com.google.cloud.bigquery.Field.Mode;
// import com.google.cloud.bigquery.LegacySQLTypeName.*;
// import com.google.cloud.bigquery.Field;
// import com.google.cloud.bigquery.Schema;
// import static com.google.common.io.ByteStreams.toByteArray;
// import com.google.protobuf.ByteString;
// import org.apache.spark.sql.catalyst.InternalRow;
// import org.apache.spark.sql.types.ArrayType;
// import org.apache.spark.sql.types.BinaryType;
//
// import org.junit.Test;
//
// public class BinaryIteratorsTest {
//
//    @Test
//    public void testCompareArrowAndAvroResults() throws Exception{
//        // rows in the form of bytes string in both arrow and avro format
//        ByteString avroByteString = ByteString.copyFrom(
//                toByteArray(getClass().getResourceAsStream("/alltypes.avro")))
//        ByteString arrowByteString = ByteString.copyFrom(
//                toByteArray(getClass().getResourceAsStream("/alltypes.arrow")))
//
//        // avro and arrow schemas required to read rows from bigquery
//        ByteString arrowSchema =
// ByteString.copyFrom(toByteArray(getClass().getResourceAsStream("/alltypes.arrowschema")))
//        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
//                parse(getClass().getResourceAsStream("/alltypes.avroschema.json"))
//
//        ImmuLi columnsInOrder = Seq("int_req", "int_null", "bl", "str", "day", "ts", "dt", "tm",
// "binary",
//                "float", "nums", "int_arr", "int_struct_arr")
//
//        Schema bqSchema = Schema.of(
//                Field.newBuilder("int_req", INTEGER).setMode(Mode.REQUIRED).build(),
//                Field.of("int_null", INTEGER),
//                Field.of("bl", BOOLEAN),
//                Field.of("str", STRING),
//                Field.of("day", DATE),
//                Field.of("ts", TIMESTAMP),
//                Field.of("dt", DATETIME),
//                Field.of("tm", TIME),
//                Field.of("binary", BYTES),
//                Field.of("float", FLOAT),
//                Field.of("nums", RECORD,
//                        Field.of("min", NUMERIC),
//                        Field.of("max", NUMERIC),
//                        Field.of("pi", NUMERIC),
//                        Field.of("big_pi", NUMERIC)),
//                Field.newBuilder("int_arr", INTEGER).setMode(Mode.REPEATED).build(),
//                Field.newBuilder("int_struct_arr", RECORD,
//                        Field.of("i", INTEGER)).setMode(Mode.REPEATED).build())
//
//        val schemaFields = SchemaConverters.toSpark(bqSchema).fields
//
//        var avroSparkRow: InternalRow = null
//        var arrowSparkRow : InternalRow = null
//
//        val arrowBinaryIterator = new ArrowBinaryIterator(columnsInOrder.asJava,
//                arrowSchema,
//                arrowByteString).asScala
//
//        if (arrowBinaryIterator.hasNext) {
//            arrowSparkRow = arrowBinaryIterator.next();
//        }
//
//        val avroBinaryIterator = new AvroBinaryIterator(bqSchema,
//                columnsInOrder.asJava, avroSchema, avroByteString)
//
//        if (avroBinaryIterator.hasNext) {
//            avroSparkRow = avroBinaryIterator.next()
//        }
//
//        for (col <- 0 to 11)
//        {
//            if (arrowSparkRow.isNullAt(col))
//            {
//                assert(avroSparkRow.isNullAt(col))
//            }
//            else
//            {
//                val schemaFieldDataType = schemaFields.apply(col).dataType
//
//                if (schemaFieldDataType == BinaryType)
//                {
//                    avroSparkRow.getBinary(col).equals(arrowSparkRow.getBinary(col))
//                }
//                else
//                if (schemaFieldDataType == (ArrayType(LongType, true)))
//                {
//                    val arr1 = avroSparkRow.getArray(col).array
//                    val arr2 = arrowSparkRow.getArray(col).array
//
//                    assert(arr1 sameElements arr2)
//                }
//                else
//                if (schemaFieldDataType.typeName == s"struct") {
//                for (fieldI <- 0 to 3) {
//                    assert(avroSparkRow.getStruct(col, 4).getDecimal(fieldI, 38, 9)
//                            .equals(arrowSparkRow.getStruct(col, 4).getDecimal(fieldI, 38, 9)))
//                }
//            }
//            else
//                {
//                    assert(avroSparkRow.get(col, schemaFieldDataType).equals(
//                            arrowSparkRow.get(col, schemaFieldDataType)))
//                }
//            }
//        }
//
//        // handling last field specially because of its complex nature
//
//        val x = arrowSparkRow.getArray(12).getStruct(0, 1).getLong(0)
//        val y = avroSparkRow.getArray(12).getStruct(0, 1).getLong(0)
//
//        assert (x == y)
//        assert(arrowBinaryIterator.hasNext == false)
//        assert(avroBinaryIterator.hasNext == false)
//    }
//
// }
