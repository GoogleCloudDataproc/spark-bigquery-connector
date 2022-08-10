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

import static com.google.common.truth.Truth.assertThat;

import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigQueryConnectorUtilsTest {

  static SparkSession spark;

  @BeforeClass
  public static void initSpark() {
    spark =
        SparkSession.builder()
            .appName(BigQueryConnectorUtilsTest.class.getName())
            .master("local")
            .getOrCreate();
  }

  @Test
  public void testEnable() throws Exception {
    assertThat(MockSparkBigQueryPushdown.enabledWasCalled).isFalse();
    BigQueryConnectorUtils.enablePushdownSession(spark);
    assertThat(MockSparkBigQueryPushdown.enabledWasCalled).isTrue();
  }

  @Test
  public void testDisable() throws Exception {
    assertThat(MockSparkBigQueryPushdown.disabledWasCalled).isFalse();
    BigQueryConnectorUtils.disablePushdownSession(spark);
    assertThat(MockSparkBigQueryPushdown.disabledWasCalled).isTrue();
  }
}
