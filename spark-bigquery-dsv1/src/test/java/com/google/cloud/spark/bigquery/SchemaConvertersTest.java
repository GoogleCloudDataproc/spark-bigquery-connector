/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat; // Google Truth import

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import org.apache.spark.sql.types.*;
import org.junit.Test;

public class SchemaConvertersTest {

  private final SchemaConverters schemaConverters =
      SchemaConverters.from(SchemaConvertersConfiguration.createDefault());

  @Test
  public void emptySchemaConversion() {
    Schema bqSchema = Schema.of();
    StructType expected = new StructType(new StructField[] {});
    StructType result = schemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void singleFieldSchemaConversion() {
    Schema bqSchema = Schema.of(Field.of("foo", LegacySQLTypeName.STRING));
    StructType expected =
        new StructType(
            new StructField[] {DataTypes.createStructField("foo", DataTypes.StringType, true)});
    StructType result = schemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void singleFieldSchemaConversionForJson() {
    Schema bqSchema = Schema.of(Field.of("foo", LegacySQLTypeName.JSON));
    Metadata metadata = new MetadataBuilder().putString("sqlType", "JSON").build();
    StructType expected =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("foo", DataTypes.StringType, true, metadata)
            });
    StructType result = schemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void fullFieldSchemaConversion() {
    Schema bqSchema =
        Schema.of(
            Field.of("foo", LegacySQLTypeName.STRING),
            Field.of("bar", LegacySQLTypeName.INTEGER),
            Field.newBuilder("required", LegacySQLTypeName.BOOLEAN)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder("binary_arr", LegacySQLTypeName.BYTES)
                .setMode(Field.Mode.REPEATED)
                .build(),
            Field.of("float", LegacySQLTypeName.FLOAT),
            Field.newBuilder("numeric", LegacySQLTypeName.NUMERIC)
                .setPrecision(38L)
                .setScale(9L)
                .build(),
            Field.of("date", LegacySQLTypeName.DATE),
            Field.of(
                "times",
                LegacySQLTypeName.RECORD,
                Field.of("time", LegacySQLTypeName.TIME),
                Field.of("timestamp", LegacySQLTypeName.TIMESTAMP),
                Field.of("datetime", LegacySQLTypeName.DATETIME)));

    StructType expected =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("foo", DataTypes.StringType, true),
              DataTypes.createStructField("bar", DataTypes.LongType, true),
              DataTypes.createStructField("required", DataTypes.BooleanType, false),
              DataTypes.createStructField(
                  "binary_arr", DataTypes.createArrayType(DataTypes.BinaryType, true), true),
              DataTypes.createStructField("float", DataTypes.DoubleType, true),
              DataTypes.createStructField("numeric", DataTypes.createDecimalType(38, 9), true),
              DataTypes.createStructField("date", DataTypes.DateType, true),
              DataTypes.createStructField(
                  "times",
                  new StructType(
                      new StructField[] {
                        DataTypes.createStructField("time", DataTypes.LongType, true),
                        DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
                        DataTypes.createStructField("datetime", DataTypes.StringType, true)
                      }),
                  true)
            });

    StructType result = schemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void fieldHasDescription() {
    Schema bqSchema =
        Schema.of(
            Field.newBuilder("name", LegacySQLTypeName.STRING)
                .setDescription("foo")
                .setMode(Field.Mode.NULLABLE)
                .build());
    Metadata expectedMetadata =
        new MetadataBuilder().putString("description", "foo").putString("comment", "foo").build();
    StructType expected =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true, expectedMetadata)
            });
    StructType result = schemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }
}
