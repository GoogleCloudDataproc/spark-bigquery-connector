package com.google.cloud.spark.bigquery.v2;

import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32CustomMetricUtils.BIG_QUERY_BYTES_READ_METRIC_NAME;
import static com.google.cloud.spark.bigquery.v2.customMetrics.Spark32CustomMetricUtils.BIG_QUERY_ROWS_READ_METRIC_NAME;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import com.google.cloud.spark.bigquery.v2.customMetrics.Spark32CustomTaskMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spark32BigQueryPartitionReader<T> extends BigQueryPartitionReader {

  public Logger log = LoggerFactory.getLogger(this.getClass());
  private InputPartitionReaderContext<T> context;

  public Spark32BigQueryPartitionReader(InputPartitionReaderContext context) {
    super(context);
    this.context = context;
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    log.info("in current metric values");
    if (!context.getBigQueryStorageReadRowsTracer().isPresent()) {
      return new Spark32CustomTaskMetric[] {};
    } else {
      BigQueryStorageReadRowsTracer bigQueryStorageReadRowsTracer =
          context.getBigQueryStorageReadRowsTracer().get();
      return new Spark32CustomTaskMetric[] {
        new Spark32CustomTaskMetric(
            BIG_QUERY_BYTES_READ_METRIC_NAME, bigQueryStorageReadRowsTracer.getBytesRead()),
        new Spark32CustomTaskMetric(
            BIG_QUERY_ROWS_READ_METRIC_NAME, bigQueryStorageReadRowsTracer.getRowsRead())
      };
    }
  }
}
