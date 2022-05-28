package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class QueryPushdownIntegrationTestBase extends SparkBigQueryIntegrationTestBase {
  @Test
  public void testDateFunctionExpressions() {
    // This table only has one row and one column which is today's date
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load("bigquery-public-data.google_political_ads.last_updated");

    df.createOrReplaceTempView("last_updated");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "report_data_updated_date, "
                    + "DATE_ADD(report_data_updated_date, 1), "
                    + "DATE_SUB(report_data_updated_date, 5), "
                    + "MONTH(report_data_updated_date), "
                    + "QUARTER(report_data_updated_date), "
                    + "YEAR(report_data_updated_date), "
                    + "TRUNC(report_data_updated_date, 'YEAR') "
                    + "FROM last_updated")
            .collectAsList();

    Row r1 = result.get(0);

    // Parsing the date rather than setting date to LocalDate.now() because the test will fail
    // in the edge case that the BigQuery read happens on an earlier date
    LocalDate date = LocalDate.parse(r1.get(0).toString());

    assertThat(r1.get(1).toString()).isEqualTo(date.plusDays(1L).toString()); // DATE_ADD
    assertThat(r1.get(2).toString()).isEqualTo(date.minusDays(5L).toString()); // DATE_SUB
    assertThat(r1.get(3)).isEqualTo(date.getMonth().getValue()); // MONTH
    assertThat(r1.get(4)).isEqualTo(date.get(IsoFields.QUARTER_OF_YEAR)); // QUARTER
    assertThat(r1.get(5)).isEqualTo(date.getYear()); // YEAR
    assertThat(r1.get(6).toString()).isEqualTo(date.with(firstDayOfYear()).toString()); // TRUNC
  }

  @Test
  public void testBasicExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "word_count & corpus_date, "
                    + "word_count | corpus_date, "
                    + "word_count ^ corpus_date, "
                    + "~ word_count "
                    + "FROM shakespeare "
                    + "WHERE word = 'augurs' AND corpus = 'sonnets'")
            .collectAsList();

    // Note that for this row, word_count equals 1 and corpus_date equals 0
    Row r1 = result.get(0);
    assertThat(r1.get(0).toString()).isEqualTo("0"); // 1 & 0
    assertThat(r1.get(1).toString()).isEqualTo("1"); // 1 | 0
    assertThat(r1.get(2).toString()).isEqualTo("1"); // 1 ^ 0
    assertThat(r1.get(3).toString()).isEqualTo("-2"); // ~1
  }
}
