package com.google.cloud.spark.bigquery.v2;

import static com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryCustomMetricConstants.*;

import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import com.google.cloud.spark.bigquery.v2.customMetrics.SparkBigQueryTaskMetric;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spark32BigQueryPartitionReader<T> extends BigQueryPartitionReader {

  public Logger log = LoggerFactory.getLogger(this.getClass());
  private InputPartitionReaderContext<T> context;
  private final OptionalLong estimatedBytesScanned;

  public Spark32BigQueryPartitionReader(InputPartitionReaderContext<T> context) {
    this(context, OptionalLong.empty());
  }

  public Spark32BigQueryPartitionReader(
      InputPartitionReaderContext<T> context, OptionalLong estimatedBytesScanned) {
    super(context);
    this.context = context;
    this.estimatedBytesScanned = estimatedBytesScanned;
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    log.trace("in current metric values");
    List<CustomTaskMetric> metrics = new ArrayList<>();
    context
        .getBigQueryStorageReadRowsTracer()
        .ifPresent(
            tracer -> {
              metrics.add(
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_BYTES_READ_METRIC_NAME, tracer.getBytesRead()));
              metrics.add(
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_ROWS_READ_METRIC_NAME, tracer.getRowsRead()));
              metrics.add(
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_SCAN_TIME_METRIC_NAME, tracer.getScanTimeInMilliSec()));
              metrics.add(
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_PARSE_TIME_METRIC_NAME, tracer.getParseTimeInMilliSec()));
              metrics.add(
                  new SparkBigQueryTaskMetric(
                      BIG_QUERY_TIME_IN_SPARK_METRIC_NAME, tracer.getTimeInSparkInMilliSec()));
              metrics.add(
                  new SparkBigQueryTaskMetric(BIG_QUERY_NUMBER_OF_READ_STREAMS_METRIC_NAME, 1));
            });
    estimatedBytesScanned.ifPresent(
        value ->
            metrics.add(
                new SparkBigQueryTaskMetric(BIG_QUERY_ESTIMATED_BYTES_SCANNED_METRIC_NAME, value)));
    return metrics.toArray(new CustomTaskMetric[0]);
  }
}
