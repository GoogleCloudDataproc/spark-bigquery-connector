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

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.spark.bigquery.TypeConverter;
import com.google.protobuf.DescriptorProtos;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class TimestampNTZTypeConverter implements TypeConverter<Long> {
  @Override
  public DataType toSparkType(LegacySQLTypeName bigQueryType) {
    if (supportsBigQueryType(bigQueryType)) {
      return DataTypes.TimestampNTZType;
    }
    throw new IllegalArgumentException("Data type not supported : " + bigQueryType);
  }

  @Override
  public LegacySQLTypeName toBigQueryType(DataType sparkType) {
    if (supportsSparkType(sparkType)) {
      return LegacySQLTypeName.DATETIME;
    }
    throw new IllegalArgumentException("Data type not supported : " + sparkType);
  }

  @Override
  public DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(DataType sparkType) {
    if (supportsSparkType(sparkType)) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
    }
    throw new IllegalArgumentException("Data type not supported : " + sparkType);
  }

  @Override
  public boolean supportsBigQueryType(LegacySQLTypeName bigQueryType) {
    return bigQueryType.getStandardType().equals(StandardSQLTypeName.DATETIME);
  }

  @Override
  public boolean supportsSparkType(DataType sparkType) {
    return sparkType.sameType(DataTypes.TimestampNTZType);
  }

  @Override
  public Long sparkToProtoValue(Object sparkValue) {
    java.time.LocalDateTime javaLocalTime = (java.time.LocalDateTime) sparkValue;

    org.threeten.bp.LocalDateTime threeTenLocalTime =
        org.threeten.bp.LocalDateTime.of(
            javaLocalTime.getYear(),
            javaLocalTime.getMonthValue(),
            javaLocalTime.getDayOfMonth(),
            javaLocalTime.getHour(),
            javaLocalTime.getMinute(),
            javaLocalTime.getSecond(),
            javaLocalTime.getNano());
    return CivilTimeEncoder.encodePacked64DatetimeMicros(threeTenLocalTime);
  }
}
