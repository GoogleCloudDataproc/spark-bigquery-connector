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
package com.google.cloud.spark.bigquery.integration;

import static com.google.cloud.spark.bigquery.integration.IntegrationTestUtils.metadata;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.from_utc_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_date;

import com.google.cloud.spark.bigquery.integration.model.ColumnOrderTestClass;
import com.google.cloud.spark.bigquery.integration.model.NumStruct;
import com.google.cloud.spark.bigquery.integration.model.StringStruct;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestConstants {

  static final String SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare";
  static final long SHAKESPEARE_TABLE_NUM_ROWS = 164656L;
  static final StructType SHAKESPEARE_TABLE_SCHEMA =
      new StructType(
          new StructField[] {
            StructField.apply(
                "word",
                DataTypes.StringType,
                false,
                metadata(
                    "description",
                    "A single unique word (where whitespace is the delimiter) extracted from a corpus.")),
            StructField.apply(
                "word_count",
                DataTypes.LongType,
                false,
                metadata("description", "The number of times this word appears in this corpus.")),
            StructField.apply(
                "corpus",
                DataTypes.StringType,
                false,
                metadata("description", "The work from which this word was extracted.")),
            StructField.apply(
                "corpus_date",
                DataTypes.LongType,
                false,
                metadata("description", "The year in which this corpus was published."))
          });
  static final String LARGE_TABLE = "bigquery-public-data.samples.natality";
  static final String LARGE_TABLE_FIELD = "is_male";
  static final long LARGE_TABLE_NUM_ROWS = 33271914L;
  static final String LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects";
  static final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
  static final String STRUCT_COLUMN_ORDER_TEST_TABLE_NAME = "struct_column_order";
  static final String ALL_TYPES_TABLE_NAME = "all_types";
  static final String ALL_TYPES_VIEW_NAME = "all_types_view";
  static DataType BQ_NUMERIC = DataTypes.createDecimalType(38, 9);
  public static int BIG_NUMERIC_COLUMN_POSITION = 11;

  public static StructType ALL_TYPES_TABLE_SCHEMA =
      new StructType(
          copy(
              new StructField("int_req", DataTypes.LongType, false, Metadata.empty()),
              new StructField("int_null", DataTypes.LongType, true, Metadata.empty()),
              new StructField("bl", DataTypes.BooleanType, true, Metadata.empty()),
              new StructField("str", DataTypes.StringType, true, Metadata.empty()),
              new StructField("day", DataTypes.DateType, true, Metadata.empty()),
              new StructField("ts", DataTypes.TimestampType, true, Metadata.empty()),
              new StructField("dt", DataTypes.StringType, true, Metadata.empty()),
              new StructField("tm", DataTypes.LongType, true, Metadata.empty()),
              new StructField("binary", DataTypes.BinaryType, true, Metadata.empty()),
              new StructField("float", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField(
                  "nums",
                  new StructType(
                      copy(
                          new StructField("min", BQ_NUMERIC, true, Metadata.empty()),
                          new StructField("max", BQ_NUMERIC, true, Metadata.empty()),
                          new StructField("pi", BQ_NUMERIC, true, Metadata.empty()),
                          new StructField("big_pi", BQ_NUMERIC, true, Metadata.empty()))),
                  true,
                  Metadata.empty()),
              // TODO: Restore this code after
              //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/446
              //  is fixed
              //
              // new StructField("big_numeric_nums",
              //     new StructType(copy(
              //         new StructField("min", BigQueryDataTypes.BigNumericType, true,
              // Metadata.empty()),
              //         new StructField("max", BigQueryDataTypes.BigNumericType, true,
              // Metadata.empty()))),
              //     true, Metadata.empty()),
              new StructField(
                  "int_arr", new ArrayType(DataTypes.LongType, true), true, Metadata.empty()),
              new StructField(
                  "int_struct_arr",
                  new ArrayType(
                      new StructType(
                          copy(new StructField("i", DataTypes.LongType, true, Metadata.empty()))),
                      true),
                  true,
                  Metadata.empty())));

  public static String ALL_TYPES_TABLE_QUERY_TEMPLATE =
      // Use DDL to support required fields
      Stream.of(
              "create table %s.%s (",
              "int_req int64 not null,",
              "int_null int64,",
              "bl bool,",
              "str string,",
              "day date,",
              "ts timestamp,",
              "dt datetime,",
              "tm time,",
              "binary bytes,",
              "float float64,",
              "nums struct<min numeric, max numeric, pi numeric, big_pi numeric>,",
              //          "big_numeric_nums struct<min bignumeric, max bignumeric>,",
              "int_arr array<int64>,",
              "int_struct_arr array<struct<i int64>>",
              ") as",
              "",
              "select",
              "42 as int_req,",
              "null as int_null,",
              "true as bl,",
              "\"string\" as str,",
              "cast(\"2019-03-18\" as date) as day,",
              "cast(\"2019-03-18T01:23:45.678901\" as timestamp) as ts,",
              "cast(\"2019-03-18T01:23:45.678901\"  as datetime) as dt,",
              "cast(\"01:23:45.678901\" as time) as tm,",
              "cast(\"bytes\" as bytes) as binary,",
              "4.2 as float,",
              "struct(",
              "  cast(\"-99999999999999999999999999999.999999999\" as numeric) as min,",
              "  cast(\"99999999999999999999999999999.999999999\" as numeric) as max,",
              "  cast(3.14 as numeric) as pi,",
              "  cast(\"31415926535897932384626433832.795028841\" as numeric) as big_pi",
              ") as nums,",
              // "struct(",
              // "
              // cast(\"-578960446186580977117854925043439539266.34992332820282019728792003956564819968\" as bignumeric) as min,",
              // "
              // cast(\"578960446186580977117854925043439539266.34992332820282019728792003956564819967\" as bignumeric) as max",
              // ") as big_numeric_nums,",
              "[1, 2, 3] as int_arr,",
              "[(select as struct 1)] as int_struct_arr")
          .collect(Collectors.joining("\n"));

  public static int ALL_TYPES_TABLE_SIZE = 160;

  static String STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE =
      Stream.of(
              "create table %s.%s (",
              "str string,",
              "nums struct <",
              "  num1 int64,",
              "  num2 int64,",
              "  num3 int64,",
              "  strings array <struct<str1 string, str2 string, str3 string>>",
              " >",
              ")",
              "as",
              "select",
              "\"outer_string\" as str,",
              "struct(",
              " 1 as num1,",
              " 2 as num2,",
              " 3 as num3,",
              " [",
              "   struct(\"0:str1\" as str1, \"0:str2\" as str2, \"0:str3\" as str3),",
              "   struct(\"1:str1\" as str1, \"1:str2\" as str2, \"1:str3\" as str3)",
              " ] as string_struct_arr",
              ") as nums")
          .collect(Collectors.joining("\n"));

  public static List<Column> ALL_TYPES_TABLE_COLS =
      Arrays.asList(
          lit(42L),
          lit(null),
          lit(true),
          lit("string"),
          to_date(lit("2019-03-18")),
          from_utc_timestamp(lit("2019-03-18T01:23:45.678901"), TimeZone.getDefault().getID()),
          lit("2019-03-18T01:23:45.678901"),
          lit(5025678901L),
          lit("bytes").cast("BINARY"),
          lit(4.2),
          struct(
              lit("-99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
              lit("99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
              lit(3.14).cast(BQ_NUMERIC),
              lit("31415926535897932384626433832.795028841").cast(BQ_NUMERIC)),
          // struct(
          //
          // lit("-578960446186580977117854925043439539266.34992332820282019728792003956564819968"),
          //
          // lit("578960446186580977117854925043439539266.34992332820282019728792003956564819967")
          // ),
          array(lit(1), lit(2), lit(3)),
          array(struct(lit(1))));

  private static <T> T[] copy(T... elements) {
    return elements;
  }

  public static ColumnOrderTestClass STRUCT_COLUMN_ORDER_TEST_TABLE_COLS =
      new ColumnOrderTestClass(
          new NumStruct(
              3L,
              2L,
              1L,
              ImmutableList.of(
                  new StringStruct("0:str3", "0:str1", "0:str2"),
                  new StringStruct("1:str3", "1:str1", "1:str2"))),
          "outer_string");
}
