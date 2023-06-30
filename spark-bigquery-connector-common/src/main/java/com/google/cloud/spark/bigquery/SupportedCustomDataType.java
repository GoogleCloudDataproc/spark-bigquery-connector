/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

public enum SupportedCustomDataType {
  SPARK_ML_VECTOR("vector", SQLDataTypes.VectorType()),
  SPARK_ML_MATRIX("matrix", SQLDataTypes.MatrixType());

  private final String typeMarker;
  private final UserDefinedType sparkDataType;

  SupportedCustomDataType(String typeMarker, DataType sparkDataType) {
    this.typeMarker = "{spark.type=" + typeMarker + "}";
    this.sparkDataType = (UserDefinedType) sparkDataType;
  }

  public UserDefinedType getSparkDataType() {
    return sparkDataType;
  }

  public String getTypeMarker() {
    return typeMarker;
  }

  public DataType getSqlType() {
    return sparkDataType.sqlType();
  }

  public static Optional<SupportedCustomDataType> of(DataType dataType) {
    Preconditions.checkNotNull(dataType);
    return Stream.of(values())
        .filter(supportedCustomDataType -> supportedCustomDataType.sparkDataType.equals(dataType))
        .findFirst();
  }

  public static Optional<SupportedCustomDataType> forDescription(String description) {
    Preconditions.checkNotNull(description, "description cannot be null");
    return Stream.of(values())
        .filter(dataType -> description.endsWith(dataType.typeMarker))
        .findFirst();
  }
}
