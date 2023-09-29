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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.DescriptorProtos;
import org.apache.spark.sql.types.DataType;

public interface TypeConverter<T> {
  DataType toSparkType(LegacySQLTypeName bigQueryType);

  LegacySQLTypeName toBigQueryType(DataType sparkType);

  DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(DataType sparkType);

  boolean supportsBigQueryType(LegacySQLTypeName bigQueryType);

  boolean supportsSparkType(DataType sparkType);

  T sparkToProtoValue(Object sparkValue);
}
