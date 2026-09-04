/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.spark.bigquery.v2.customMetrics;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkBigQueryEstimatedBytesScannedMetricTest {
  private final SparkBigQueryEstimatedBytesScannedMetric metric =
      new SparkBigQueryEstimatedBytesScannedMetric();

  @Test
  public void testName() {
    assertThat(metric.name()).isEqualTo("bqEstimatedBytesScanned");
  }

  @Test
  public void testDescription() {
    assertThat(metric.description())
        .isEqualTo("estimated logical bytes scanned by BigQuery Storage API");
  }

  @Test
  public void testAggregateMetricsUsesMaximum() {
    assertThat(metric.aggregateTaskMetrics(new long[] {1024L, 4096L, 2048L})).isEqualTo("4.0 KiB");
  }

  @Test
  public void testAggregateMetricsDoesNotMultiplyRepeatedSessionEstimate() {
    assertThat(metric.aggregateTaskMetrics(new long[] {4096L, 4096L, 4096L})).isEqualTo("4.0 KiB");
  }

  @Test
  public void testAggregateMetricsWithNoTasksIsZero() {
    assertThat(metric.aggregateTaskMetrics(new long[] {})).isEqualTo("0.0 B");
  }
}
