package com.google.cloud.spark.bigquery;

import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.types.DataType;

/**
 * This helper is to avoid accessing {@link SupportedCustomDataType} class if not necessary. That
 * class is mostly to support Spark ML specific types. That forces the user's code to include Spark
 * ML library to avoid run-time exception even though the code doesn't need Spark ML library.
 */
public enum SupportedCustomDataTypeHelper {
  SPARK_ML_VECTOR("vector"),
  SPARK_ML_MATRIX("matrix");

  private final String typeName;
  private final String typeMarker;

  SupportedCustomDataTypeHelper(String typeName) {
    this.typeName = typeName;
    this.typeMarker = "{spark.type=" + typeName + "}";
  }

  public static Optional<SupportedCustomDataType> of(DataType dataType) {
    Preconditions.checkNotNull(dataType);
    Optional<SupportedCustomDataTypeHelper> foundValue =
        Stream.of(values())
            .filter(
                supportedCustomDataType ->
                    supportedCustomDataType.typeName.equals(dataType.typeName()))
            .findFirst();
    return foundValue.map(SupportedCustomDataTypeHelper::toSupportedCustomDataType);
  }

  public static Optional<SupportedCustomDataType> forDescription(String description) {
    Preconditions.checkNotNull(description, "description cannot be null");
    Optional<SupportedCustomDataTypeHelper> foundValue =
        Stream.of(values())
            .filter(dataType -> description.endsWith(dataType.typeMarker))
            .findFirst();
    return foundValue.map(SupportedCustomDataTypeHelper::toSupportedCustomDataType);
  }

  private static SupportedCustomDataType toSupportedCustomDataType(
      SupportedCustomDataTypeHelper type) {
    if (type.equals(SPARK_ML_VECTOR)) return SupportedCustomDataType.SPARK_ML_VECTOR;
    else return SupportedCustomDataType.SPARK_ML_MATRIX;
  }
}
