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

import java.util.TimeZone

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TestConstants {
  private val BQ_NUMERIC = DataTypes.createDecimalType(38, 9)

  val ALL_TYPES_TABLE_SCHEMA = StructType(Seq(
    StructField("int_req", LongType, nullable = false),
    StructField("int_null", LongType, nullable = true),
    StructField("bl", BooleanType, nullable = true),
    StructField("str", StringType, nullable = true),
    StructField("day", DateType, nullable = true),
    StructField("ts", TimestampType, nullable = true),
    StructField("dt", StringType, nullable = true),
    StructField("tm", LongType, nullable = true),
    StructField("binary", BinaryType, nullable = true),
    StructField("float", DoubleType, nullable = true),
    StructField("nums",
      StructType(Seq(
        StructField("min", BQ_NUMERIC, nullable = true),
        StructField("max", BQ_NUMERIC, nullable = true),
        StructField("pi", BQ_NUMERIC, nullable = true),
        StructField("big_pi", BQ_NUMERIC, nullable = true))),
      nullable = true),
    StructField("int_arr", ArrayType(LongType, containsNull = true), nullable = true),
    StructField("int_struct_arr", ArrayType(
      StructType(Seq(StructField("i", LongType, nullable = true))), containsNull = true),
      nullable = true)
  ))
  val ALL_TYPES_TABLE_QUERY_TEMPLATE: String =
  // Use DDL to support required fields
    """
      |create table %s (
      |int_req int64 not null,
      |int_null int64,
      |bl bool,
      |str string,
      |day date,
      |ts timestamp,
      |dt datetime,
      |tm time,
      |binary bytes,
      |float float64,
      |nums struct<min numeric, max numeric, pi numeric, big_pi numeric>,
      |int_arr array<int64>,
      |int_struct_arr array<struct<i int64>>
      |) as
      |
      |select
      |42 as int_req,
      |null as int_null,
      |true as bl,
      |"string" as str,
      |cast("2019-03-18" as date) as day,
      |cast("2019-03-18T01:23:45.678901" as timestamp) as ts,
      |cast("2019-03-18T01:23:45.678901"  as datetime) as dt,
      |cast("01:23:45.678901" as time) as tm,
      |cast("bytes" as bytes) as binary,
      |4.2 as float,
      |struct(
      |  cast("-99999999999999999999999999999.999999999" as numeric) as min,
      |  cast("99999999999999999999999999999.999999999" as numeric) as max,
      |  cast(3.14 as numeric) as pi,
      |  cast("31415926535897932384626433832.795028841" as numeric) as big_pi
      |) as nums,
      |[1, 2, 3] as int_arr,
      |[(select as struct 1)] as int_struct_arr
    """.stripMargin

  val ALL_TYPES_TABLE_SIZE = 160

  val ALL_TYPES_TABLE_COLS: Seq[Column] = Seq(
    lit(42L),
    lit(null),
    lit(true),
    lit("string"),
    to_date(lit("2019-03-18")),
    from_utc_timestamp(lit("2019-03-18T01:23:45.678901"), TimeZone.getDefault.getID),
    lit("2019-03-18T01:23:45.678901"),
    lit(5025678901L),
    lit("bytes").cast("BINARY"),
    lit(4.2),
    struct(
      lit("-99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
      lit("99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
      lit(3.14).cast(BQ_NUMERIC),
      lit("31415926535897932384626433832.795028841").cast(BQ_NUMERIC)),
    array(lit(1), lit(2), lit(3)),
    array(struct(lit(1)))
  )

  val STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE =
    """
      |create table %s (
      |str string,
      |nums struct <
      |  num1 int64,
      |  num2 int64,
      |  num3 int64,
      |  string_struct_arr array <struct<str1 string, str2 string, str3 string>>
      | >
      |)
      |as
      |select
      |"outer_string" as str,
      |struct(
      | 1 as num1,
      | 2 as num2,
      | 3 as num3,
      | [
      |   struct("0:str1" as str1, "0:str2" as str2, "0:str3" as str3),
      |   struct("1:str1" as str1, "1:str2" as str2, "1:str3" as str3)
      | ] as string_struct_arr
      |) as nums
      """.stripMargin

  val STRUCT_COLUMN_ORDER_TEST_TABLE_COLS =
    ColumnOrderTestClass(
      NumStruct(
        3,
        2,
        1,
        List(
          StringStruct("0:str3", "0:str1", "0:str2"),
          StringStruct("1:str3", "1:str1", "1:str2")
        )),
      "outer_string")

  case class StringStruct(str3: String, str1: String, str2: String)
  case class NumStruct(num3: Long, num2: Long, num1: Long, string_struct_arr: List[StringStruct])
  case class ColumnOrderTestClass(nums: NumStruct, str: String)
}
