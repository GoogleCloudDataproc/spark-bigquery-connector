package com.google.cloud.spark.bigquery.okera;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

public class RecordServiceConfTest {
  @Test
  public void testLoadOkeraConfigFromSqlContext() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.not.okera", "foo")
            .set("spark.recordservice.planner.hostports", "bar");

    SparkContext sc = mock(SparkContext.class);
    when(sc.getConf()).thenReturn(sparkConf);

    SQLContext sql = mock(SQLContext.class);
    when(sql.sparkContext()).thenReturn(sc);
    when(sql.getConf(anyString(), any())).then(invocation -> invocation.getArgument(1));
    when(sql.getConf(eq("spark.okera.not"), any())).thenReturn("foofoo");
    when(sql.getConf(eq("spark.recordservice.workers.local-port"), any())).thenReturn("barbar");

    Configuration conf = RecordServiceConf.fromSQLContext(sql);
    assertEquals("bar", conf.get("recordservice.planner.hostports"));
    assertEquals("barbar", conf.get("recordservice.workers.local-port"));

    assertNull(conf.get("spark.not.okera"));
    assertNull(conf.get("spark.okera.not"));
    assertNull(conf.get("spark.recordservice.planner.hostports"));
    assertNull(conf.get("spark.recordservice.workers.local-port"));
  }
}
