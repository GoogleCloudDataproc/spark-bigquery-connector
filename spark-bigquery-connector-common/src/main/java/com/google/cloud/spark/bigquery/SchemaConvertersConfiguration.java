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

import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.common.base.Objects;
import java.io.Serializable;

public class SchemaConvertersConfiguration implements Serializable {

  private static final long serialVersionUID = -5971771984509805208L;
  private final boolean allowMapTypeConversion;
  private final int bigNumericDefaultPrecision;
  private final int bigNumericDefaultScale;
  private final boolean enableArrowTimestampRebase;

  private SchemaConvertersConfiguration(
      boolean allowMapTypeConversion,
      int bigNumericDefaultPrecision,
      int bigNumericDefaultScale,
      boolean enableArrowTimestampRebase) {
    this.allowMapTypeConversion = allowMapTypeConversion;
    this.bigNumericDefaultPrecision = bigNumericDefaultPrecision;
    this.bigNumericDefaultScale = bigNumericDefaultScale;
    this.enableArrowTimestampRebase = enableArrowTimestampRebase;
  }

  public static SchemaConvertersConfiguration from(SparkBigQueryConfig config) {
    return SchemaConvertersConfiguration.of(
        config.getAllowMapTypeConversion(),
        config.getBigNumericDefaultPrecision(),
        config.getBigNumericDefaultScale(),
        config.getEnableArrowTimestampRebase());
  }

  public static SchemaConvertersConfiguration of(boolean allowMapTypeConversion) {
    return new SchemaConvertersConfiguration(
        allowMapTypeConversion,
        BigQueryUtil.DEFAULT_BIG_NUMERIC_PRECISION,
        BigQueryUtil.DEFAULT_BIG_NUMERIC_SCALE,
        true);
  }

  public static SchemaConvertersConfiguration of(
      boolean allowMapTypeConversion, int bigNumericDefaultPrecision, int bigNumericDefaultScale) {
    return new SchemaConvertersConfiguration(
        allowMapTypeConversion, bigNumericDefaultPrecision, bigNumericDefaultScale, true);
  }

  public static SchemaConvertersConfiguration of(
      boolean allowMapTypeConversion,
      int bigNumericDefaultPrecision,
      int bigNumericDefaultScale,
      boolean enableArrowTimestampRebase) {
    return new SchemaConvertersConfiguration(
        allowMapTypeConversion,
        bigNumericDefaultPrecision,
        bigNumericDefaultScale,
        enableArrowTimestampRebase);
  }

  public static SchemaConvertersConfiguration createDefault() {
    return new SchemaConvertersConfiguration(
        SparkBigQueryConfig.ALLOW_MAP_TYPE_CONVERSION_DEFAULT,
        BigQueryUtil.DEFAULT_BIG_NUMERIC_PRECISION,
        BigQueryUtil.DEFAULT_BIG_NUMERIC_SCALE,
        true);
  }

  public boolean getAllowMapTypeConversion() {
    return allowMapTypeConversion;
  }

  public int getBigNumericDefaultPrecision() {
    return bigNumericDefaultPrecision;
  }

  public int getBigNumericDefaultScale() {
    return bigNumericDefaultScale;
  }

  public boolean getEnableArrowTimestampRebase() {
    return enableArrowTimestampRebase;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaConvertersConfiguration that = (SchemaConvertersConfiguration) o;
    return Objects.equal(allowMapTypeConversion, that.allowMapTypeConversion)
        && Objects.equal(bigNumericDefaultPrecision, that.bigNumericDefaultPrecision)
        && Objects.equal(bigNumericDefaultScale, that.bigNumericDefaultScale)
        && Objects.equal(enableArrowTimestampRebase, that.enableArrowTimestampRebase);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        allowMapTypeConversion,
        bigNumericDefaultPrecision,
        bigNumericDefaultScale,
        enableArrowTimestampRebase);
  }

  @Override
  public String toString() {
    return "SchemaConvertersConfiguration{"
        + "allowMapTypeConversion="
        + allowMapTypeConversion
        + ", enableArrowTimestampRebase="
        + enableArrowTimestampRebase
        + '}';
  }
}
