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
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import com.google.cloud.spark.bigquery.integration.model.ColumnOrderTestClass;
import com.google.cloud.spark.bigquery.integration.model.NumStruct;
import com.google.cloud.spark.bigquery.integration.model.StringStruct;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestConstants {

  static final int BQ_NUMERIC_PRECISION = 38;
  static final int BQ_NUMERIC_SCALE = 9;
  static final int BQ_BIGNUMERIC_SCALE = 38;
  static final DecimalType NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);
  static final DecimalType BIGNUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_BIGNUMERIC_SCALE);
  static final String SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare";
  static final String PUBLIC_DATA_WIKIPEDIA_PAGEVIEWS_2021 =
      "bigquery-public-data.wikipedia.pageviews_2021";
  static final long SHAKESPEARE_TABLE_NUM_ROWS = 164656L;
  static final String TEMPORARY_GCS_BUCKET_ENV_VARIABLE = "TEMPORARY_GCS_BUCKET";
  static final String BIGLAKE_CONNECTION_ID_ENV_VARIABLE = "BIGLAKE_CONNECTION_ID";
  static final String TEMPORARY_GCS_BUCKET =
      Preconditions.checkNotNull(
          System.getenv(TEMPORARY_GCS_BUCKET_ENV_VARIABLE),
          "Please set the %s env variable to point to a write enabled GCS bucket",
          TEMPORARY_GCS_BUCKET_ENV_VARIABLE);
  static final String BIGLAKE_CONNECTION_ID =
      Preconditions.checkNotNull(
          System.getenv(BIGLAKE_CONNECTION_ID_ENV_VARIABLE),
          "Please set the BIGLAKE_CONNECTION_ID env variable in order to create biglake table");
  static final String SHAKESPEARE_CSV_FILENAME = "shakespeare.csv";
  static final String SHAKESPEARE_JSON_FILENAME = "shakespeare.json";
  static final String SHAKESPEARE_AVRO_FILENAME = "shakespeare.avro";
  static final String SHAKESPEARE_PARQUET_FILENAME = "shakespeare.parquet";
  static final StructType SHAKESPEARE_TABLE_SCHEMA =
      new StructType(
          new StructField[] {
            StructField.apply(
                "word",
                DataTypes.StringType,
                false,
                metadata(
                    "description",
                    "A single unique word (where whitespace is the delimiter) extracted from a"
                        + " corpus.")),
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
  static final String ALL_TYPES_VIEW_NAME = "shakespeare_view";
  static final String DIFF_IN_SCHEMA_SRC_TABLE_NAME = "src_table";
  static final String DIFF_IN_SCHEMA_SRC_TABLE_NAME_WITH_DESCRIPTION = "src_table_with_description";
  static final String DIFF_IN_SCHEMA_DEST_TABLE_NAME = "dest_table";
  static DataType BQ_NUMERIC = DataTypes.createDecimalType(38, 9);
  static DataType BQ_BIGNUMERIC = DataTypes.createDecimalType(38, 38);
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
                  "json_f",
                  DataTypes.StringType,
                  true,
                  Metadata.fromJson("{\"sqlType\":\"JSON\"}")),
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
              new StructField(
                  "big_numeric_nums",
                  new StructType(
                      copy(
                          new StructField("min", BQ_BIGNUMERIC, true, Metadata.empty()),
                          new StructField("max", BQ_BIGNUMERIC, true, Metadata.empty()))),
                  true,
                  Metadata.empty()),
              new StructField(
                  "int_arr", new ArrayType(DataTypes.LongType, true), true, Metadata.empty()),
              new StructField(
                  "int_struct_arr",
                  new ArrayType(
                      new StructType(
                          copy(new StructField("i", DataTypes.LongType, true, Metadata.empty()))),
                      true),
                  true,
                  Metadata.empty()),
              new StructField(
                  "nested_struct",
                  new StructType(
                      copy(
                          new StructField("int_null", DataTypes.LongType, true, Metadata.empty()),
                          new StructField("str", DataTypes.StringType, true, Metadata.empty()),
                          new StructField(
                              "inner_struct",
                              new StructType(
                                  copy(
                                      new StructField(
                                          "bl", DataTypes.BooleanType, true, Metadata.empty()),
                                      new StructField(
                                          "str", DataTypes.StringType, true, Metadata.empty()),
                                      new StructField(
                                          "int_null", DataTypes.LongType, true, Metadata.empty()))),
                              true,
                              Metadata.empty()))),
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
              "json_f json,",
              "nums struct<min numeric, max numeric, pi numeric, big_pi numeric>,",
              "big_numeric_nums struct<min bignumeric(38,38), max bignumeric(38,38)>,",
              "int_arr array<int64>,",
              "int_struct_arr array<struct<i int64>>,",
              "nested_struct struct<int_null int64, str string, inner_struct struct<bl bool, str string, int_null int64>>",
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
              "JSON \'{\"json\":\"true\"}\' as json_f,",
              "struct(",
              "  cast(\"-99999999999999999999999999999.999999999\" as numeric) as min,",
              "  cast(\"99999999999999999999999999999.999999999\" as numeric) as max,",
              "  cast(3.14 as numeric) as pi,",
              "  cast(\"31415926535897932384626433832.795028841\" as numeric) as big_pi",
              ") as nums,",
              "struct(",
              "cast(\"-0.34992332820282019728792003956564819968\" as bignumeric) as min,",
              "cast(\"0.34992332820282019728792003956564819967\" as bignumeric) as max",
              ") as big_numeric_nums,",
              "[1, 2, 3] as int_arr,",
              "[(select as struct 1)] as int_struct_arr,",
              "struct(",
              "  1 as int_null,",
              "  \"stringa\" as str,",
              "  struct( true as bl, \"stringaa\" as str, 11 as int_null) ",
              ") as nested_struct")
          .collect(Collectors.joining("\n"));

  public static int ALL_TYPES_TABLE_SIZE = 266;
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

  public static String DIFF_IN_SCHEMA_SRC_TABLE =
      Stream.of(
              "create table %s.%s (",
              "int_req int64 not null,",
              "int_null int64,",
              ") as",
              "",
              "select",
              "42 as int_req,",
              "null as int_null")
          .collect(Collectors.joining("\n"));

  public static String DIFF_IN_SCHEMA_SRC_TABLE_WITH_DESCRIPTION =
      Stream.of(
              "create table %s.%s (",
              "int_req int64 not null,",
              "int_null int64 OPTIONS(description='An INTEGER field with description'),",
              ") as",
              "",
              "select",
              "42 as int_req,",
              "null as int_null")
          .collect(Collectors.joining("\n"));

  public static String DIFF_IN_SCHEMA_DEST_TABLE =
      Stream.of("create table %s.%s (", "int_req int64,", "int_null int64,", ")", "")
          .collect(Collectors.joining("\n"));

  public static String DIFF_IN_SCHEMA_DEST_TABLE_WITH_NULLABLE_FIELD =
      Stream.of("create table %s.%s (", "int_null int64,", ")", "")
          .collect(Collectors.joining("\n"));

  public static String DIFF_IN_SCHEMA_DEST_TABLE_WITH_REQUIRED_FIELD =
      Stream.of("create table %s.%s (", "int_req int64 not null,", ")", "")
          .collect(Collectors.joining("\n"));

  public static String DIFF_IN_SCHEMA_DEST_TABLE_WITH_REPEATED_FIELD =
      Stream.of("create table %s.%s (", "int_rep ARRAY<INT64>,", ")", "")
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
          lit("{\"json\":\"true\"}"),
          struct(
              lit("-99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
              lit("99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
              lit(3.14).cast(BQ_NUMERIC),
              lit("31415926535897932384626433832.795028841").cast(BQ_NUMERIC)),
          struct(
              lit("-0.34992332820282019728792003956564819968").cast(BQ_BIGNUMERIC),
              lit("0.34992332820282019728792003956564819967").cast(BQ_BIGNUMERIC)),
          array(lit(1), lit(2), lit(3)),
          array(struct(lit(1))),
          struct(lit(1), lit("stringa"), struct(lit(true), lit("stringaa"), lit(11))));

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

  // Constants for storage api tests.
  public static final StructType STORAGE_API_ALL_TYPES_SCHEMA =
      new StructType()
          .add(
              new StructField(
                  "int_req",
                  IntegerType,
                  false,
                  new MetadataBuilder().putString("description", "required integer").build()))
          .add(new StructField("int_null", IntegerType, true, Metadata.empty()))
          .add(new StructField("long", LongType, true, Metadata.empty()))
          .add(new StructField("short", ShortType, true, Metadata.empty()))
          .add(new StructField("bytenum", ByteType, true, Metadata.empty()))
          .add(new StructField("bool", BooleanType, true, Metadata.empty()))
          .add(new StructField("str", StringType, true, Metadata.empty()))
          .add(new StructField("date", DateType, true, Metadata.empty()))
          .add(new StructField("timestamp", TimestampType, true, Metadata.empty()))
          .add(new StructField("binary", BinaryType, true, Metadata.empty()))
          .add(new StructField("float", DoubleType, true, Metadata.empty()))
          .add(new StructField("json_f", StringType, true, Metadata.empty()))
          .add(
              new StructField(
                  "nums",
                  new StructType()
                      .add(new StructField("min", NUMERIC_SPARK_TYPE, true, Metadata.empty()))
                      .add(new StructField("max", NUMERIC_SPARK_TYPE, true, Metadata.empty()))
                      .add(new StructField("pi", NUMERIC_SPARK_TYPE, true, Metadata.empty()))
                      .add(new StructField("big_pi", NUMERIC_SPARK_TYPE, true, Metadata.empty())),
                  true,
                  Metadata.empty()))
          .add(
              new StructField(
                  "big_numeric_nums",
                  new StructType()
                      .add(new StructField("min", BQ_BIGNUMERIC, true, Metadata.empty()))
                      .add(new StructField("max", BQ_BIGNUMERIC, true, Metadata.empty())),
                  true,
                  Metadata.empty()))
          .add(new StructField("int_arr", new ArrayType(IntegerType, true), true, Metadata.empty()))
          .add(
              new StructField(
                  "int_struct_arr",
                  new ArrayType(
                      new StructType()
                          .add(new StructField("i", IntegerType, true, Metadata.empty())),
                      true),
                  true,
                  Metadata.empty()));

  // same as ALL_TYPES_SCHEMA, except all IntegerType's are LongType's.
  public static final StructType STORAGE_API_ALL_TYPES_SCHEMA_BIGQUERY_REPRESENTATION =
      new StructType()
          .add(
              new StructField(
                  "int_req",
                  LongType,
                  false,
                  new MetadataBuilder()
                      .putString("description", "required integer")
                      .putString("comment", "required integer")
                      .build()))
          .add(new StructField("int_null", LongType, true, Metadata.empty()))
          .add(new StructField("long", LongType, true, Metadata.empty()))
          .add(new StructField("short", LongType, true, Metadata.empty()))
          .add(new StructField("bytenum", LongType, true, Metadata.empty()))
          .add(new StructField("bool", BooleanType, true, Metadata.empty()))
          .add(new StructField("str", StringType, true, Metadata.empty()))
          .add(new StructField("date", DateType, true, Metadata.empty()))
          .add(new StructField("timestamp", TimestampType, true, Metadata.empty()))
          .add(new StructField("binary", BinaryType, true, Metadata.empty()))
          .add(new StructField("float", DoubleType, true, Metadata.empty()))
          .add(new StructField("json_f", StringType, true, Metadata.empty()))
          .add(
              new StructField(
                  "nums",
                  new StructType()
                      .add(new StructField("min", NUMERIC_SPARK_TYPE, true, Metadata.empty()))
                      .add(new StructField("max", NUMERIC_SPARK_TYPE, true, Metadata.empty()))
                      .add(new StructField("pi", NUMERIC_SPARK_TYPE, true, Metadata.empty()))
                      .add(new StructField("big_pi", NUMERIC_SPARK_TYPE, true, Metadata.empty())),
                  true,
                  Metadata.empty()))
          .add(
              new StructField(
                  "big_numeric_nums",
                  new StructType()
                      .add(new StructField("min", BQ_BIGNUMERIC, true, Metadata.empty()))
                      .add(new StructField("max", BQ_BIGNUMERIC, true, Metadata.empty())),
                  true,
                  Metadata.empty()))
          .add(new StructField("int_arr", new ArrayType(LongType, true), true, Metadata.empty()))
          .add(
              new StructField(
                  "int_struct_arr",
                  new ArrayType(
                      new StructType().add(new StructField("i", LongType, true, Metadata.empty())),
                      true),
                  true,
                  Metadata.empty()));

  public static final Row[] STORAGE_API_ALL_TYPES_ROWS =
      new Row[] {
        RowFactory.create(
            123456789,
            null,
            123456789L,
            (short) 1024,
            (byte) 127,
            true,
            "hello",
            Date.valueOf("2019-03-18"),
            new Timestamp(1552872225000L), // 2019-03-18 01:23:45
            new byte[] {
              98, 121, 116, 101, 115
            }, // byte[] representation of string "bytes" -> stored in BQ as Ynl0ZXM=
            1.2345,
            "{\"json\":\"true\"}",
            RowFactory.create(
                Decimal.apply(
                    new BigDecimal(
                        "-99999999999999999999999999999.999999999",
                        new MathContext(BQ_NUMERIC_PRECISION)),
                    BQ_NUMERIC_PRECISION,
                    BQ_NUMERIC_SCALE),
                Decimal.apply(
                    new BigDecimal(
                        "99999999999999999999999999999.999999999",
                        new MathContext(BQ_NUMERIC_PRECISION)),
                    BQ_NUMERIC_PRECISION,
                    BQ_NUMERIC_SCALE),
                Decimal.apply(
                    new BigDecimal("3.14", new MathContext(BQ_NUMERIC_PRECISION)),
                    BQ_NUMERIC_PRECISION,
                    BQ_NUMERIC_SCALE),
                Decimal.apply(
                    new BigDecimal(
                        "31415926535897932384626433832.795028841",
                        new MathContext(BQ_NUMERIC_PRECISION)),
                    BQ_NUMERIC_PRECISION,
                    BQ_NUMERIC_SCALE)),
            RowFactory.create(
                Decimal.apply(
                    new BigDecimal("-0.34992332820282019728792003956564819968"),
                    BQ_NUMERIC_PRECISION,
                    BQ_BIGNUMERIC_SCALE),
                Decimal.apply(
                    new BigDecimal("0.34992332820282019728792003956564819967"),
                    BQ_NUMERIC_PRECISION,
                    BQ_BIGNUMERIC_SCALE)),
            new int[] {1, 2, 3, 4},
            new Row[] {RowFactory.create(1), RowFactory.create(1)})
      };

  public static final List<NumStruct> numStructDataset =
      Arrays.asList(
          new NumStruct(
              1L, 2L, 3L, ImmutableList.of(new StringStruct("1:str3", "2:str1", "3:str2"))),
          new NumStruct(
              2L, 3L, 4L, ImmutableList.of(new StringStruct("2:str3", "3:str1", "4:str2"))));

  public static final List<NumStruct> numStructDatasetForJoin =
      Arrays.asList(
          new NumStruct(
              1L, 4L, 3L, ImmutableList.of(new StringStruct("3:str3", "4:str4", "3:str3"))),
          new NumStruct(
              3L, 5L, 6L, ImmutableList.of(new StringStruct("6:str6", "5:str5", "3:str3"))),
          new NumStruct(
              3L, 1L, 4L, ImmutableList.of(new StringStruct("3:str3", "1:str1", "4:str4"))));

  public static final List<NumStruct> numStructDatasetForWindow =
      Arrays.asList(
          new NumStruct(
              1L, 3L, 4L, ImmutableList.of(new StringStruct("1:str3", "2:str1", "3:str2"))),
          new NumStruct(
              2L, 2L, 6L, ImmutableList.of(new StringStruct("2:str3", "3:str1", "4:str2"))),
          new NumStruct(
              1L, 3L, 5L, ImmutableList.of(new StringStruct("1:str3", "2:str1", "3:str2"))),
          new NumStruct(
              2L, 2L, 7L, ImmutableList.of(new StringStruct("2:str3", "3:str1", "4:str2"))));
}
