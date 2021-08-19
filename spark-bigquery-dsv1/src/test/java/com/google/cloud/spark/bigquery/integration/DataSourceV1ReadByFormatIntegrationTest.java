package com.google.cloud.spark.bigquery.integration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.spark.bigquery.BigNumeric;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.collection.JavaConverters;

import static com.google.common.truth.Truth.assertThat;

@RunWith(Parameterized.class)
public class DataSourceV1ReadByFormatIntegrationTest extends ReadByFormatIntegrationTestBase {

  static IntegrationTestContext ctx;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> formats() {
    return Arrays.asList(new Object[][]{
        {"AVRO"},
        {"ARROW"}
    });
  }

  public DataSourceV1ReadByFormatIntegrationTest(String format) {
    super(ctx, format);
  }

  @BeforeClass
  public static void initialize() {
    ctx = IntegrationTestUtils.initialize(DataSourceV1ReadByFormatIntegrationTest.class, true);
  }

  @AfterClass
  public static void clean() {
    IntegrationTestUtils.clean(ctx);
  }

}
