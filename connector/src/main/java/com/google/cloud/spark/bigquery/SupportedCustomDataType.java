package com.google.cloud.spark.bigquery;

import com.google.common.base.Preconditions;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.types.DataType;

import java.util.Optional;
import java.util.stream.Stream;

public enum SupportedCustomDataType {
  SPARK_ML_VECTOR("vector", SQLDataTypes.VectorType()),
  SPARK_ML_MATRIX("matrix", SQLDataTypes.MatrixType());

  private final String typeMarker;
  private final DataType sparkDataType;

  SupportedCustomDataType(String typeMarker, DataType sparkDataType) {
    this.typeMarker = "{spark.type=" + typeMarker + "}";
    this.sparkDataType = sparkDataType;
  }

  public DataType getSparkDataType() {
    return sparkDataType;
  }

  public String getTypeMarker() {
    return typeMarker;
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
