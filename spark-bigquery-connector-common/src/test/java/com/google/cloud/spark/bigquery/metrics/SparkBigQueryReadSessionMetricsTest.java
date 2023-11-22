package com.google.cloud.spark.bigquery.metrics;

import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.integration.TestConstants;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeThat;

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
    if(sparkContext != null) {
      sparkContext.stop();
    }
  }

  @Test
  public void testReadSessionMetricsAccumulator() {
    String sessionName = "testSession";
    long numReadStreams = 10;
    SparkBigQueryReadSessionMetrics metrics =
        SparkBigQueryReadSessionMetrics.from(
            sparkContext,
            ReadSession.newBuilder().setName(sessionName).build(),
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

  private boolean checkIfReadSessionMetricEventExist() {
    try {
      Class.forName(
          "com.google.cloud.spark.events.BigQueryConnectorReadSessionMetricEvent$BigQueryConnectorReadSessionMetricEventBuilder");
      return true;
    } catch (ClassNotFoundException ignored) {
      return false;
    }
  }

  @Test
  public void testPostReadSessionMetricsToBus() {
    assumeThat(checkIfReadSessionMetricEventExist(), equalTo(true));
    String sessionName = "testSession";
    long numReadStreams = 10;
    SparkBigQueryReadSessionMetrics metrics =
            SparkBigQueryReadSessionMetrics.from(
                    sparkContext,
                    ReadSession.newBuilder().setName(sessionName).build(),
                    numReadStreams);

    sparkContext.addSparkListener(metrics);
    spark.read().format("bigquery").option("table", "bigquery-public-data.samples.shakespeare").load().count();
    System.out.println(metrics.getBytesRead());
  }
}
