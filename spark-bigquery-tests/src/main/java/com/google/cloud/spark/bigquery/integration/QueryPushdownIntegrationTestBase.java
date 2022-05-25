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
  public void testStringFunctionExpressions() {
    Dataset<Row> df = spark.read().format("bigquery").load(TestConstants.SHAKESPEARE_TABLE);
    df =
        df.selectExpr(
                "word",
                "ASCII(word) as ascii",
                "LENGTH(word) as length",
                "LOWER(word) as lower",
                "LPAD(word, 10, '*') as lpad",
                "RPAD(word, 10, '*') as rpad",
                "TRANSLATE(word, 'a', '*') as translate",
                "TRIM(concat('    ', word, '    ')) as trim",
                "LTRIM(concat('    ', word, '    ')) as ltrim",
                "RTRIM(concat('    ', word, '    ')) as rtrim",
                "UPPER(word) as upper",
                "INSTR(word, 'a') as instr",
                "INITCAP(word) as initcap",
                "CONCAT(word, '*', '!!') as concat",
                "BASE64(word) as base",
                "UNBASE64(word) as base",
                "FORMAT_STRING('*%s*', word) as format_string",
                "FORMAT_NUMBER(10.2345, 1) as format_number",
                "REGEXP_EXTRACT(word, '([A-Za-z]+$)', 1) as regexp_extract",
                "REGEXP_REPLACE(word, '([A-Za-z]+$)', 'replacement') as regexp_replace",
                "SUBSTR(word, 2, 2) as substr",
                "SOUNDEX(word) as soundex")
            .where("word = 'augurs'");
    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("augurs"); // word
    assertThat(r1.get(1)).isEqualTo(97); // ASCII(word)
    assertThat(r1.get(2)).isEqualTo(6); // LENGTH(word)
    assertThat(r1.get(3)).isEqualTo("augurs"); // LOWER(word)
    assertThat(r1.get(4)).isEqualTo("****augurs"); // LPAD(word, 10, '*')
    assertThat(r1.get(5)).isEqualTo("augurs****"); // LPAD(word, 10, '*')
    assertThat(r1.get(6)).isEqualTo("*ugurs"); // TRANSLATE(word, 'a', '*')
    assertThat(r1.get(7)).isEqualTo("augurs"); // TRIM(concat('    ', word, '    '))
    assertThat(r1.get(8)).isEqualTo("augurs    "); // LTRIM(concat('    ', word, '    '))
    assertThat(r1.get(9)).isEqualTo("    augurs"); // RTRIM(concat('    ', word, '    '))
    assertThat(r1.get(10)).isEqualTo("AUGURS"); // UPPER(word)
    assertThat(r1.get(11)).isEqualTo(1); // INSTR(word, 'a')
    assertThat(r1.get(12)).isEqualTo("Augurs"); // INITCAP(word)
    assertThat(r1.get(13)).isEqualTo("augurs*!!"); // CONCAT(word, '*', '!!')
    assertThat(r1.get(16)).isEqualTo("*augurs*"); // FORMAT_STRING('*%s*', word)
    assertThat(r1.get(17)).isEqualTo("10.2"); // FORMAT_NUMBER(10.2345, 1)
    assertThat(r1.get(18)).isEqualTo("augurs"); // REGEXP_EXTRACT(word, '([A-Za-z]+$)', 1)
    assertThat(r1.get(19))
        .isEqualTo("replacement"); // REGEXP_REPLACE(word, '([A-Za-z]+$)', 'replacement')
    assertThat(r1.get(20)).isEqualTo("ug"); // SUBSTR(word, 2, 2)
    assertThat(r1.get(21)).isEqualTo("A262"); // SOUNDEX(word)
  }

  @Test
  public void testDateFunctionExpressions() {
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
}
