package com.google.cloud.spark.bigquery;

import org.apache.spark.ml.linalg.SQLDataTypes;
import org.junit.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

public class SupportedCustomDataTypeTest {

  @Test
  public void testVector() throws Exception {
    Optional<SupportedCustomDataType> vector =
        SupportedCustomDataType.of(SQLDataTypes.VectorType());
    assertThat(vector.isPresent()).isTrue();
  }

  @Test
  public void testMatrix() throws Exception {
    Optional<SupportedCustomDataType> matrix =
        SupportedCustomDataType.of(SQLDataTypes.MatrixType());
    assertThat(matrix.isPresent()).isTrue();
  }
}
