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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryNumRowsBeforePruningMetric;
import com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryTaskMetric;
import com.google.common.collect.ObjectArrays;
import java.util.OptionalLong;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Scan builder for Spark 3.4+, reporting the pre-pruning table row count as driver metrics ({@code
 * numRowsBeforePruning} and the {@code rawInputRows} alias) when known, and omitting both from
 * {@link #supportedCustomMetrics()} and {@link #reportDriverMetrics()} when unknown.
 */
public class Spark34BigQueryScanBuilder extends Spark33BigQueryScanBuilder {

  private static final CustomMetric[] NUM_ROWS_BEFORE_PRUNING_METRICS = {
    new SparkBigQueryNumRowsBeforePruningMetric(),
    SparkBigQueryNumRowsBeforePruningMetric.rawInputRowsAlias()
  };

  private final OptionalLong numRowsBeforePruning;

  public Spark34BigQueryScanBuilder(BigQueryDataSourceReaderContext ctx) {
    super(ctx);
    this.numRowsBeforePruning = ctx.getNumRowsBeforePruning();
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    CustomMetric[] baseMetrics = super.supportedCustomMetrics();
    if (!numRowsBeforePruning.isPresent()) {
      return baseMetrics;
    }
    return ObjectArrays.concat(baseMetrics, NUM_ROWS_BEFORE_PRUNING_METRICS, CustomMetric.class);
  }

  @Override
  public CustomTaskMetric[] reportDriverMetrics() {
    CustomTaskMetric[] baseMetrics = super.reportDriverMetrics();
    if (!numRowsBeforePruning.isPresent()) {
      return baseMetrics;
    }
    long rows = numRowsBeforePruning.getAsLong();
    CustomTaskMetric[] rowCountMetrics = {
      new SparkBigQueryTaskMetric(NUM_ROWS_BEFORE_PRUNING_METRICS[0].name(), rows),
      new SparkBigQueryTaskMetric(NUM_ROWS_BEFORE_PRUNING_METRICS[1].name(), rows)
    };
    return ObjectArrays.concat(baseMetrics, rowCountMetrics, CustomTaskMetric.class);
  }
}
