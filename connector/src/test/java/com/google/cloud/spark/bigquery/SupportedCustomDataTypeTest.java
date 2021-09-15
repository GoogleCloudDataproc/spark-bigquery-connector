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

import org.apache.spark.bigquery.BigNumeric;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
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

  @Test
  public void testSerializabilityOfBigNumeric() throws IOException {
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(new BigNumeric(new BigDecimal("123.123")));
  }

  @Test
  public void testBigNumericEquality() throws IOException {
    BigNumeric bn1 = new BigNumeric(new BigDecimal("123.123"));
    BigNumeric bn2 = new BigNumeric(new BigDecimal("123.123"));
    BigNumeric bn3 = new BigNumeric(new BigDecimal("456.456"));

    assertThat(bn1).isEqualTo(bn2);
    assertThat(bn1).isNotEqualTo(bn3);
  }

  @Test
  public void testBigNumericNullEquality() throws IOException {
    BigNumeric bn1 = new BigNumeric(new BigDecimal("123.123"));
    BigNumeric bn2 = new BigNumeric(null);
    BigNumeric bn3 = new BigNumeric(null);

    assertThat(bn1).isNotEqualTo(bn2);
    assertThat(bn2).isEqualTo(bn3);
  }

  @Test
  public void testBigNumericHashCode() throws IOException {
    BigNumeric bn1 = new BigNumeric(new BigDecimal("123.123"));
    BigNumeric bn2 = new BigNumeric(new BigDecimal("123.123"));
    BigNumeric bn3 = new BigNumeric(new BigDecimal("456.456"));

    assertThat(bn1.hashCode()).isEqualTo(bn2.hashCode());
    assertThat(bn1.hashCode()).isNotEqualTo(bn3.hashCode());
  }

  @Test
  public void testBigNumericNullHashCode() throws IOException {
    BigNumeric bn1 = new BigNumeric(new BigDecimal("123.123"));
    BigNumeric bn2 = new BigNumeric(null);
    BigNumeric bn3 = new BigNumeric(null);

    assertThat(bn1.hashCode()).isNotEqualTo(bn2.hashCode());
    assertThat(bn2.hashCode()).isEqualTo(bn3.hashCode());
  }
}
