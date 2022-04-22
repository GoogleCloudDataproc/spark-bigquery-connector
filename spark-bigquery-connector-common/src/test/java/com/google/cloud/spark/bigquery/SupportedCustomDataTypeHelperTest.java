package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat;

import java.util.Optional;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class SupportedCustomDataTypeHelperTest {

  @Test
  public void testVectorFromDescription() {
    Optional<SupportedCustomDataType> vector =
        SupportedCustomDataTypeHelper.forDescription("{spark.type=vector}");
    assertThat(vector.isPresent()).isTrue();
    assertThat(vector.get())
        .isEquivalentAccordingToCompareTo(SupportedCustomDataType.SPARK_ML_VECTOR);
  }

  @Test
  public void testMatrixFromDescription() {
    Optional<SupportedCustomDataType> matrix =
        SupportedCustomDataTypeHelper.forDescription("{spark.type=matrix}");
    assertThat(matrix.isPresent()).isTrue();
    assertThat(matrix.get())
        .isEquivalentAccordingToCompareTo(SupportedCustomDataType.SPARK_ML_MATRIX);
  }

  @Test
  public void testUnsupportedFromDescription() {
    Optional<SupportedCustomDataType> unsupported =
        SupportedCustomDataTypeHelper.forDescription("{spark.type=unsupported}");
    assertThat(unsupported.isPresent()).isFalse();
  }

  @Test
  public void testOfDataType() {
    Optional<SupportedCustomDataType> matrix =
        SupportedCustomDataTypeHelper.of(SQLDataTypes.MatrixType());
    assertThat(matrix.isPresent()).isTrue();
    assertThat(matrix.get())
        .isEquivalentAccordingToCompareTo(SupportedCustomDataType.SPARK_ML_MATRIX);

    Optional<SupportedCustomDataType> notCustom =
        SupportedCustomDataTypeHelper.of(DataTypes.StringType);
    assertThat(notCustom.isPresent()).isFalse();
  }
}
