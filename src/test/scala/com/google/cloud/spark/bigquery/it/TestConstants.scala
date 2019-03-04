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
package com.google.cloud.spark.bigquery.it

import org.apache.spark.sql.types._

object TestConstants {
  // TODO(#3): replace with dynamically created table (this one is private).
  val KNOWN_TABLE = "pc.all_types"
  val KNOWN_TABLE_SCHEMA = StructType(Seq(
    StructField("int_req", LongType, nullable = false),
    StructField("int_null", LongType, nullable = true),
    StructField("bl", BooleanType, nullable = true),
    StructField("str", StringType, nullable = true),
    StructField("day", DateType, nullable = true),
    StructField("ts", TimestampType, nullable = true),
    StructField("dt", StringType, nullable = true),
    StructField("tm", LongType, nullable = true),
    StructField("float", DoubleType, nullable = true),
    StructField("binary", BinaryType, nullable = true),
    StructField("int_arr", ArrayType(LongType, containsNull = false), nullable = true),
    StructField("int_struct",
      StructType(Seq(
        // Java client mistakenly writes subfields wrong??
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true),
        StructField("c", LongType, nullable = true))),
      nullable = true),
    StructField("int_struct_arr", ArrayType(
      StructType(Seq(StructField("i", LongType, nullable = true))), containsNull = false),
      nullable = true)
  ))
  val KNOWN_TABLE_SIZE = 120
}
