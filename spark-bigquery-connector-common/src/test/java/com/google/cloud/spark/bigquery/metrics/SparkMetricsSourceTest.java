package com.google.cloud.spark.bigquery.metrics;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class SparkMetricsSourceTest {

  @Test
  public void testNew() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource("");
    assertThat(sparkMetricsSource.sourceName()).isEqualTo("bigquery-metrics-source");
  }

  @Test
  public void testParseTime() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource("");
    assertThat(sparkMetricsSource.getParseTime().getCount()).isEqualTo(0);
    sparkMetricsSource.updateParseTime(10);
    assertThat(sparkMetricsSource.getParseTime().getCount()).isEqualTo(1);
  }

  @Test
  public void testTimeInSpark() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource("");
    assertThat(sparkMetricsSource.getTimeInSpark().getCount()).isEqualTo(0);
    sparkMetricsSource.updateTimeInSpark(12);
    sparkMetricsSource.updateTimeInSpark(56);
    sparkMetricsSource.updateTimeInSpark(200);
    assertThat(sparkMetricsSource.getTimeInSpark().getCount()).isEqualTo(3);
  }

  @Test
  public void testBytesReadCounter() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource("");
    assertThat(sparkMetricsSource.getBytesRead().getCount()).isEqualTo(0);
    sparkMetricsSource.incrementBytesReadCounter(1);
    sparkMetricsSource.incrementBytesReadCounter(2);
    sparkMetricsSource.incrementBytesReadCounter(3);
    assertThat(sparkMetricsSource.getBytesRead().getCount()).isEqualTo(6);
  }

  @Test
  public void testRowsReadCounter() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource("");
    assertThat(sparkMetricsSource.getRowsRead().getCount()).isEqualTo(0);
    sparkMetricsSource.incrementRowsReadCounter(2);
    sparkMetricsSource.incrementRowsReadCounter(3);
    sparkMetricsSource.incrementRowsReadCounter(4);
    assertThat(sparkMetricsSource.getRowsRead().getCount()).isEqualTo(9);
  }

  @Test
  public void testScanTime() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource("");
    assertThat(sparkMetricsSource.getScanTime().getCount()).isEqualTo(0);
    sparkMetricsSource.updateScanTime(22);
    sparkMetricsSource.updateScanTime(56);
    assertThat(sparkMetricsSource.getScanTime().getCount()).isEqualTo(2);
  }
}
