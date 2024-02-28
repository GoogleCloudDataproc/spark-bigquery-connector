package com.google.cloud.spark.bigquery.v2;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;

import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryTaskMetric;
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
    log.trace("in current metric values");
    return context
        .getBigQueryStorageReadRowsTracer()
        .map(
            bigQueryStorageReadRowsTracer ->
                new SparkBigQueryTaskMetric[] {
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_BYTES_READ_METRIC_NAME,
                      bigQueryStorageReadRowsTracer.getBytesRead()),
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_ROWS_READ_METRIC_NAME, bigQueryStorageReadRowsTracer.getRowsRead()),
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_SCAN_TIME_METRIC_NAME,
                      bigQueryStorageReadRowsTracer.getScanTimeInMilliSec()),
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_PARSE_TIME_METRIC_NAME,
                      bigQueryStorageReadRowsTracer.getParseTimeInMilliSec()),
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_TIME_IN_SPARK_METRIC_NAME,
                      bigQueryStorageReadRowsTracer.getTimeInSparkInMilliSec()),
                  new SparkBigQueryTaskMetric(BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_NAME, 1)
                })
        .orElse(new SparkBigQueryTaskMetric[] {});
  }
}
