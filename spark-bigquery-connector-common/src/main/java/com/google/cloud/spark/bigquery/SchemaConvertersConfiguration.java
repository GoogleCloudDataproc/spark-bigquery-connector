/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.empty;
import static com.google.common.base.Optional.fromJavaUtil;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import java.io.Serializable;

public class SchemaConvertersConfiguration implements Serializable {

  private final boolean allowMapTypeConversion;
  private Optional<Integer> bigNumericFieldsPrecision = empty();
  private Optional<Integer> bigNumericFieldsScale = empty();

  private SchemaConvertersConfiguration(
      boolean allowMapTypeConversion,
      Optional<Integer> bigNumericFieldsPrecision,
      Optional<Integer> bigNumericFieldsScale) {
    this.allowMapTypeConversion = allowMapTypeConversion;
    this.bigNumericFieldsPrecision = bigNumericFieldsPrecision;
    this.bigNumericFieldsScale = bigNumericFieldsScale;
  }

  public static SchemaConvertersConfiguration from(SparkBigQueryConfig config) {
    return SchemaConvertersConfiguration.of(
        config.getAllowMapTypeConversion(),
        fromJavaUtil(config.getBigNumericFieldsPrecision()),
        fromJavaUtil(config.getBigNumericFieldsScale()));
  }

  public static SchemaConvertersConfiguration of(boolean allowMapTypeConversion) {
    return new SchemaConvertersConfiguration(allowMapTypeConversion, empty(), empty());
  }

  public static SchemaConvertersConfiguration of(
      boolean allowMapTypeConversion,
      Optional<Integer> bigNumericFieldsPrecision,
      Optional<Integer> bigNumericFieldsScale) {
    return new SchemaConvertersConfiguration(
        allowMapTypeConversion, bigNumericFieldsPrecision, bigNumericFieldsScale);
  }

  public static SchemaConvertersConfiguration createDefault() {
    return new SchemaConvertersConfiguration(
        SparkBigQueryConfig.ALLOW_MAP_TYPE_CONVERSION_DEFAULT, empty(), empty());
  }

  public boolean getAllowMapTypeConversion() {
    return allowMapTypeConversion;
  }

  public Optional<Integer> getBigNumericFieldsPrecision() {
    return bigNumericFieldsPrecision;
  }

  public Optional<Integer> getBigNumericFieldsScale() {
    return bigNumericFieldsScale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaConvertersConfiguration that = (SchemaConvertersConfiguration) o;
    return Objects.equal(allowMapTypeConversion, that.allowMapTypeConversion)
        && Objects.equal(bigNumericFieldsPrecision, that.bigNumericFieldsPrecision)
        && Objects.equal(bigNumericFieldsScale, that.bigNumericFieldsScale);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        allowMapTypeConversion, bigNumericFieldsPrecision, bigNumericFieldsScale);
  }

  @Override
  public String toString() {
    return "SchemaConvertersConfiguration{"
        + "allowMapTypeConversion="
        + allowMapTypeConversion
        + '}';
  }
}
