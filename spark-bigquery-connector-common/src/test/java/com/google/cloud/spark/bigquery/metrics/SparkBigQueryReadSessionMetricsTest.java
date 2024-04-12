package com.google.cloud.spark.bigquery.metrics;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SparkBigQueryReadSessionMetricsTest {
  SparkSession spark;

  SparkContext sparkContext;

  @Before
  public void setup() throws Throwable {
    spark =
        SparkSession.builder()
            .master("local")
            .config("spark.default.parallelism", 20)
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
    sparkContext = spark.sparkContext();
  }

  @After
  public void tearDown() {
    if (sparkContext != null) {
      sparkContext.stop();
    }
  }

  @Test
  public void testReadSessionMetricsAccumulator() {
    String sessionName = "projects/test-project/locations/us/sessions/testSession";
    long numReadStreams = 10;
    SparkBigQueryReadSessionMetrics metrics =
        SparkBigQueryReadSessionMetrics.from(
            spark,
            ReadSession.newBuilder().setName(sessionName).build(),
            10L,
            DataFormat.ARROW,
            DataOrigin.QUERY,
            numReadStreams);
    assertThat(metrics.getNumReadStreams()).isEqualTo(numReadStreams);

    metrics.incrementBytesReadAccumulator(1024);
    metrics.incrementBytesReadAccumulator(2048);
    assertThat(metrics.getBytesRead()).isEqualTo(3072);

    metrics.incrementRowsReadAccumulator(1);
    metrics.incrementRowsReadAccumulator(4);
    assertThat(metrics.getRowsRead()).isEqualTo(5);

    metrics.incrementParseTimeAccumulator(1000);
    metrics.incrementParseTimeAccumulator(5000);
    assertThat(metrics.getParseTime()).isEqualTo(6000);

    metrics.incrementScanTimeAccumulator(1000);
    metrics.incrementScanTimeAccumulator(5000);
    assertThat(metrics.getScanTime()).isEqualTo(6000);
  }
}
