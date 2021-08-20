package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation;
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
    ctx = IntegrationTestUtils.initialize(DataSourceV1ReadByFormatIntegrationTest.class);
  }

  @AfterClass
  public static void clean() {
    IntegrationTestUtils.clean(ctx);
  }

  @Test
  public void testOptimizedCountStarWithFilter() {
    DirectBigQueryRelation.emptyRowRDDsCreated_$eq(0);
    long oldMethodCount = spark.read().format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("optimizedEmptyProjection", "false")
        .option("readDataFormat", dataFormat)
        .load()
        .select("corpus_date")
        .where("corpus_date > 0")
        .count();

    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated()).isEqualTo(0);

    long optimizedCount = spark.read().format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", dataFormat)
        .load()
        .where("corpus_date > 0")
        .count();

    assertThat(optimizedCount).isEqualTo(oldMethodCount);
    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated()).isEqualTo(1);
  }

  @Test
  public void testOptimizedCountStar() {
    DirectBigQueryRelation.emptyRowRDDsCreated_$eq(0);
    long oldMethodCount = spark.read().format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("optimizedEmptyProjection", "false")
        .option("readDataFormat", dataFormat)
        .load()
        .select("corpus_date")
        .count();

    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated()).isEqualTo(0);

    long optimizedCount = spark.read().format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", dataFormat)
        .load()
        .count();

    assertThat(optimizedCount).isEqualTo(oldMethodCount);
    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated()).isEqualTo(1);
  }
}
