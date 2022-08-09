/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdown;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.ServiceLoader;
import org.apache.spark.sql.SparkSession;

public class BigQueryConnectorUtils {

  private BigQueryConnectorUtils() {}

  private static Supplier<SparkBigQueryPushdown> sparkBigQueryPushdownSupplier =
      Suppliers.memoize(BigQueryConnectorUtils::createSparkBigQueryPushdown);

  public static void enablePushdownSession(SparkSession spark) {
    SparkBigQueryPushdown sparkBigQueryPushdown = sparkBigQueryPushdownSupplier.get();
    sparkBigQueryPushdown.enable(spark);
  }

  public static void disablePushdownSession(SparkSession spark) {
    SparkBigQueryPushdown sparkBigQueryPushdown = sparkBigQueryPushdownSupplier.get();
    sparkBigQueryPushdown.disable(spark);
  }

  static SparkBigQueryPushdown createSparkBigQueryPushdown() {
    // We won't have two spark versions in the same process, so the actual session does not matter
    String sparkVersion = SparkSession.active().version();
    ServiceLoader<SparkBigQueryPushdown> loader = ServiceLoader.load(SparkBigQueryPushdown.class);
    for (SparkBigQueryPushdown p : loader) {
      if (p.supportsSparkVersion(sparkVersion)) {
        return p;
      }
    }
    throw new IllegalStateException(
        String.format(
            "Could not find an implementation of %s that supports Spark version %s",
            SparkBigQueryPushdown.class.getName(), sparkVersion));
  }
}
