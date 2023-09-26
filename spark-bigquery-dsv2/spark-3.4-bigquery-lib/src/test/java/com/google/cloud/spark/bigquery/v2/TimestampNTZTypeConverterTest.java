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

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.protobuf.DescriptorProtos;
import java.time.LocalDateTime;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class TimestampNTZTypeConverterTest {
  private final TimestampNTZTypeConverter timestampNTZTypeConverter =
      new TimestampNTZTypeConverter();

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
}
