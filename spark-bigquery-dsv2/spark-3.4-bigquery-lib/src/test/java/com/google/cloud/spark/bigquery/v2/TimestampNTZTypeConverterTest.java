/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.v2;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;

public class TimestampNTZTypeConverterTest {
  private final TimestampNTZTypeConverter timestampNTZTypeConverter =
      new TimestampNTZTypeConverter();
  private static final SchemaConverters SCHEMA_CONVERTERS =
      SchemaConverters.from(SchemaConvertersConfiguration.createDefault());

  @Test
  public void testToSparkType() {
    assertThat(timestampNTZTypeConverter.toSparkType(LegacySQLTypeName.DATETIME))
        .isEqualTo(DataTypes.TimestampNTZType);
  }

  @Test
  public void testToSparkTypeThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          timestampNTZTypeConverter.toSparkType(LegacySQLTypeName.FLOAT);
        });
  }

  @Test
  public void testToBigQueryType() {
    assertThat(timestampNTZTypeConverter.toBigQueryType(DataTypes.TimestampNTZType))
        .isEqualTo(LegacySQLTypeName.DATETIME);
  }

  @Test
  public void testToBigQueryTypeThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          timestampNTZTypeConverter.toBigQueryType(DataTypes.TimestampType);
        });
  }

  @Test
  public void testToProtoFieldType() {
    assertThat(timestampNTZTypeConverter.toProtoFieldType(DataTypes.TimestampNTZType))
        .isEqualTo(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
  }

  @Test
  public void testToProtoFieldTypeThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          timestampNTZTypeConverter.toProtoFieldType(DataTypes.TimestampType);
        });
  }

  @Test
  public void testSupportsBigQueryType() {
    assertThat(timestampNTZTypeConverter.supportsBigQueryType(LegacySQLTypeName.DATETIME))
        .isEqualTo(true);
    assertThat(timestampNTZTypeConverter.supportsBigQueryType(LegacySQLTypeName.TIMESTAMP))
        .isEqualTo(false);
  }

  @Test
  public void testSupportsSparkType() {
    assertThat(timestampNTZTypeConverter.supportsSparkType(DataTypes.TimestampNTZType))
        .isEqualTo(true);
    assertThat(timestampNTZTypeConverter.supportsSparkType(DataTypes.TimestampType))
        .isEqualTo(false);
  }

  @Test
  public void testSparkToProtoValue() {
    LocalDateTime javaLocalTime = LocalDateTime.of(2023, 9, 18, 14, 30, 15, 234 * 1_000_000);
    long protoDateTime = timestampNTZTypeConverter.sparkToProtoValue(javaLocalTime);
    org.threeten.bp.LocalDateTime threeTenLocalTime =
        org.threeten.bp.LocalDateTime.of(
            javaLocalTime.getYear(),
            javaLocalTime.getMonthValue(),
            javaLocalTime.getDayOfMonth(),
            javaLocalTime.getHour(),
            javaLocalTime.getMinute(),
            javaLocalTime.getSecond(),
            javaLocalTime.getNano());
    assertThat(threeTenLocalTime)
        .isEqualTo(CivilTimeEncoder.decodePacked64DatetimeMicros(protoDateTime));
  }

  @Test
  public void testAvroToSparkValue_formatsAndEdgeCases() {
    long expected2024Micros =
        LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123456000).toInstant(ZoneOffset.UTC).getEpochSecond()
                * 1_000_000L
            + 123456L;

    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("2024-01-02T03:04:05.123456")))
        .isEqualTo(expected2024Micros);
    assertThat(timestampNTZTypeConverter.avroToSparkValue("2024-01-02T03:04:05.123456"))
        .isEqualTo(expected2024Micros);
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("2024-01-02 03:04:05.123456")))
        .isEqualTo(expected2024Micros);

    // Non-CharSequence fallback via toString()
    Object customObj =
        new Object() {
          @Override
          public String toString() {
            return "2024-01-02T03:04:05.123456";
          }
        };
    assertThat(timestampNTZTypeConverter.avroToSparkValue(customObj)).isEqualTo(expected2024Micros);

    // Variable fractional digits (5 digits) and no fractional digits
    long expectedFiveDigits =
        LocalDateTime.of(2019, 11, 11, 11, 11, 11, 111110000)
                    .toInstant(ZoneOffset.UTC)
                    .getEpochSecond()
                * 1_000_000L
            + 111110L;
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("2019-11-11T11:11:11.11111")))
        .isEqualTo(expectedFiveDigits);

    long expectedNoFraction =
        LocalDateTime.of(2026, 3, 19, 5, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond()
            * 1_000_000L;
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("2026-03-19T05:00:00")))
        .isEqualTo(expectedNoFraction);

    // Epoch and pre-1970 with fractional microseconds
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("1970-01-01T00:00:00")))
        .isEqualTo(0L);
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("1969-12-31T23:59:59.123456")))
        .isEqualTo(-876544L);

    // BigQuery DATETIME boundaries
    long minMicros =
        LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000L;
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("0001-01-01T00:00:00")))
        .isEqualTo(minMicros);

    long maxMicros =
        LocalDateTime.of(9999, 12, 31, 23, 59, 59).toInstant(ZoneOffset.UTC).getEpochSecond()
                * 1_000_000L
            + 999999L;
    assertThat(timestampNTZTypeConverter.avroToSparkValue(new Utf8("9999-12-31T23:59:59.999999")))
        .isEqualTo(maxMicros);

    assertThrows(
        DateTimeParseException.class,
        () -> timestampNTZTypeConverter.avroToSparkValue(new Utf8("not-a-datetime")));
  }

  @Test
  public void testConvertToInternalRow_withInferredAndEmptyAndStringSchemas() {
    Schema bqSchema =
        Schema.of(
            Field.of("dt", LegacySQLTypeName.DATETIME),
            Field.newBuilder("dt_array", LegacySQLTypeName.DATETIME).setMode(Mode.REPEATED).build(),
            Field.of(
                "dt_struct",
                LegacySQLTypeName.RECORD,
                Field.of("nested_dt", LegacySQLTypeName.DATETIME)),
            Field.newBuilder("null_dt", LegacySQLTypeName.DATETIME).setMode(Mode.NULLABLE).build());

    org.apache.avro.Schema nestedAvroSchema =
        SchemaBuilder.record("dt_struct")
            .fields()
            .name("nested_dt")
            .type()
            .stringType()
            .noDefault()
            .endRecord();
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("root")
            .fields()
            .name("dt")
            .type()
            .stringType()
            .noDefault()
            .name("dt_array")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .name("dt_struct")
            .type(nestedAvroSchema)
            .noDefault()
            .name("null_dt")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .endRecord();

    GenericRecord nestedRecord = new GenericData.Record(nestedAvroSchema);
    nestedRecord.put("nested_dt", new Utf8("2024-01-02T03:04:05.123456"));

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("dt", new Utf8("2026-03-19T05:00:00"));
    avroRecord.put(
        "dt_array",
        Arrays.asList(new Utf8("1970-01-01T00:00:00"), new Utf8("2026-03-19T05:00:00")));
    avroRecord.put("dt_struct", nestedRecord);
    avroRecord.put("null_dt", null);

    List<String> namesInOrder = ImmutableList.of("dt", "dt_array", "dt_struct", "null_dt");
    long expectedTopMicros = 1773896400000000L; // 2026-03-19T05:00:00Z
    long expectedNestedMicros = 1704164645123456L; // 2024-01-02T03:04:05.123456Z

    // 1. DSv2 Spark 3.4+ runtime path: inferred schema (with TimestampNTZType) passed as
    // Optional.of(inferredSchema) via Spark31BigQueryTable -> BigQueryDataSourceReaderContext
    StructType inferredSchema = SCHEMA_CONVERTERS.toSpark(bqSchema);
    assertThat(inferredSchema.apply("dt").dataType()).isEqualTo(DataTypes.TimestampNTZType);

    InternalRow rowWithInferredSchema =
        SCHEMA_CONVERTERS.convertToInternalRow(
            bqSchema, namesInOrder, avroRecord, Optional.of(inferredSchema));
    assertThat(rowWithInferredSchema.getLong(0)).isEqualTo(expectedTopMicros);
    assertThat(rowWithInferredSchema.getArray(1).toLongArray())
        .asList()
        .containsExactly(0L, expectedTopMicros)
        .inOrder();
    assertThat(rowWithInferredSchema.getStruct(2, 1).getLong(0)).isEqualTo(expectedNestedMicros);
    assertThat(rowWithInferredSchema.isNullAt(3)).isTrue();

    // 2. Direct convertToInternalRow call with Optional.empty() (userProvidedField == null)
    InternalRow rowWithEmptySchema =
        SCHEMA_CONVERTERS.convertToInternalRow(
            bqSchema, namesInOrder, avroRecord, Optional.empty());
    assertThat(rowWithEmptySchema.getLong(0)).isEqualTo(expectedTopMicros);
    assertThat(rowWithEmptySchema.getArray(1).toLongArray())
        .asList()
        .containsExactly(0L, expectedTopMicros)
        .inOrder();
    assertThat(rowWithEmptySchema.getStruct(2, 1).getLong(0)).isEqualTo(expectedNestedMicros);
    assertThat(rowWithEmptySchema.isNullAt(3)).isTrue();

    // 3. Explicit StringType schema (e.g. DSv1 BigQueryRDDContext): preserves UTF8String
    StructType stringOverrideSchema =
        new StructType(
            new StructField[] {
              new StructField("dt", DataTypes.StringType, true, Metadata.empty()),
              new StructField(
                  "dt_array",
                  DataTypes.createArrayType(DataTypes.StringType, true),
                  true,
                  Metadata.empty()),
              new StructField(
                  "dt_struct",
                  new StructType(
                      new StructField[] {
                        new StructField("nested_dt", DataTypes.StringType, true, Metadata.empty())
                      }),
                  true,
                  Metadata.empty()),
              new StructField("null_dt", DataTypes.StringType, true, Metadata.empty())
            });

    InternalRow rowWithStringSchema =
        SCHEMA_CONVERTERS.convertToInternalRow(
            bqSchema, namesInOrder, avroRecord, Optional.of(stringOverrideSchema));
    assertThat(rowWithStringSchema.getUTF8String(0))
        .isEqualTo(UTF8String.fromString("2026-03-19T05:00:00"));
    assertThat(rowWithStringSchema.getStruct(2, 1).getUTF8String(0))
        .isEqualTo(UTF8String.fromString("2024-01-02T03:04:05.123456"));
    assertThat(rowWithStringSchema.isNullAt(3)).isTrue();
  }
}
