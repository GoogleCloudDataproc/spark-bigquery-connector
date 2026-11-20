/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import com.google.common.collect.ImmutableList;

/**
 * Test values for every BigQuery scalar type, computed from a row number in both BigQuery SQL and
 * Spark SQL, so that a test can generate the same rows on both sides and compare them.
 */
final class DataTypeTestValues {

  /** A BigQuery scalar type and the expressions that compute a value of it from a row number. */
  static final class ScalarType {
    /** Suffix for the names of the columns that hold this type. */
    final String name;

    final String bigQueryType;
    private final String bigQueryTemplate;
    private final String sparkTemplate;

    ScalarType(String name, String bigQueryType, String bigQueryTemplate, String sparkTemplate) {
      this.name = name;
      this.bigQueryType = bigQueryType;
      this.bigQueryTemplate = bigQueryTemplate;
      this.sparkTemplate = sparkTemplate;
    }

    /** A BigQuery SQL expression for the value of row {@code rowNumber} (a SQL expression). */
    String bigQueryValue(String rowNumber) {
      return bigQueryTemplate.replace("{x}", rowNumber);
    }

    /** The Spark SQL expression that computes the same value as {@link #bigQueryValue}. */
    String sparkValue(String rowNumber) {
      return sparkTemplate.replace("{x}", rowNumber);
    }
  }

  static final ImmutableList<ScalarType> SCALAR_TYPES =
      ImmutableList.of(
          new ScalarType("bool", "BOOL", "MOD(({x}), 2) = 0", "({x}) % 2 = 0"),
          new ScalarType("int64", "INT64", "({x}) * 1000", "({x}) * 1000"),
          new ScalarType("float64", "FLOAT64", "({x}) / 8", "CAST(({x}) AS DOUBLE) / 8"),
          new ScalarType(
              "numeric",
              "NUMERIC",
              "CAST(({x}) AS NUMERIC) / 1000",
              "CAST(CAST(({x}) AS DECIMAL(20, 0)) / 1000 AS DECIMAL(38, 9))"),
          new ScalarType(
              "bignumeric",
              "BIGNUMERIC(38, 38)",
              "CAST(({x}) AS BIGNUMERIC) / 1000000",
              "CAST(CAST(({x}) AS DECIMAL(20, 0)) / 1000000 AS DECIMAL(38, 38))"),
          new ScalarType(
              "string",
              "STRING",
              "CONCAT('s', CAST(({x}) AS STRING))",
              "concat('s', CAST(({x}) AS STRING))"),
          new ScalarType(
              "bytes",
              "BYTES",
              "CAST(CONCAT('b', CAST(({x}) AS STRING)) AS BYTES)",
              "CAST(concat('b', CAST(({x}) AS STRING)) AS BINARY)"),
          new ScalarType(
              "timestamp",
              "TIMESTAMP",
              "TIMESTAMP_MICROS(1577836800000000 + ({x}) * 1000003)",
              "timestamp_micros(1577836800000000 + ({x}) * 1000003)"),
          new ScalarType(
              "date",
              "DATE",
              "DATE_ADD(DATE '2020-01-01', INTERVAL ({x}) DAY)",
              "date_add(DATE'2020-01-01', CAST(({x}) AS INT))"),
          new ScalarType(
              "datetime",
              "DATETIME",
              "DATETIME_ADD(DATETIME '2020-01-01 00:00:00', INTERVAL ({x}) * 1000003 MICROSECOND)",
              "TIMESTAMP_NTZ'2020-01-01 00:00:00'"
                  + " + make_dt_interval(0, 0, 0, CAST(({x}) * 1000003 / 1000000.0 AS DECIMAL(18, 6)))"),
          new ScalarType(
              "time",
              "TIME",
              "TIME_ADD(TIME '00:00:00', INTERVAL ({x}) * 1000003 MICROSECOND)",
              "({x}) * 1000003"),
          new ScalarType(
              "json",
              "JSON",
              "PARSE_JSON(CONCAT('{\"n\":', CAST(({x}) AS STRING), '}'))",
              "concat('{\"n\":', CAST(({x}) AS STRING), '}')"));

  private DataTypeTestValues() {}
}
