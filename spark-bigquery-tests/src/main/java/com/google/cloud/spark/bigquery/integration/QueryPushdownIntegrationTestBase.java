package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.IsoFields;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class QueryPushdownIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  @Test
  public void testStringFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
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
                    + "report_data_updated_time, "
                    + "DATE_ADD(report_data_updated_time, 1), "
                    + "DATE_SUB(report_data_updated_time, 5), "
                    + "MONTH(report_data_updated_time), "
                    + "QUARTER(report_data_updated_time), "
                    + "YEAR(report_data_updated_time), "
                    + "TRUNC(report_data_updated_time, 'YEAR') "
                    + "FROM last_updated")
            .collectAsList();

    Row r1 = result.get(0);

    // Parsing the date rather than setting date to LocalDate.now() because the test will fail
    // in the edge case that the BigQuery read happens on an earlier date
    LocalDate date = LocalDateTime.parse(r1.get(0).toString()).toLocalDate();

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

  @Test
  public void testMathematicalFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df =
        df.selectExpr(
                "word",
                "word_count",
                "ABS(-22) as Abs",
                "ACOS(1) as Acos",
                "ASIN(0) as Asin",
                "ROUND(ATAN(0.5),2) as Atan",
                "COS(0) as Cos",
                "COSH(0) as Cosh",
                "ROUND(EXP(1),2) as Exp",
                "FLOOR(EXP(1)) as Floor",
                "GREATEST(1,5,3,4) as Greatest",
                "LEAST(1,5,3,4) as Least",
                "ROUND(LOG(word_count, 2.71), 2) as Log",
                "ROUND(LOG10(word_count), 2) as Log10",
                "POW(word_count, 2) as Pow",
                "ROUND(RAND(10),2) as Rand",
                "SIN(0) as Sin",
                "SINH(0) as Sinh",
                "ROUND(SQRT(word_count), 2) as sqrt",
                "TAN(0) as Tan",
                "TANH(0) as Tanh",
                "ISNAN(word_count) as IsNan",
                "SIGNUM(word_count) as Signum")
            .where("word_count = 10 and word = 'glass'");
    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(10); // word_count
    assertThat(r1.get(2)).isEqualTo(22); // ABS(-22)
    assertThat(r1.get(3)).isEqualTo(0.0); // ACOS(1)
    assertThat(r1.get(4)).isEqualTo(0.0); // ASIN(0)
    assertThat(r1.get(5)).isEqualTo(0.46); // ROUND(ATAN(0.5),2)
    assertThat(r1.get(6)).isEqualTo(1.0); // COS(0)
    assertThat(r1.get(7)).isEqualTo(1.0); // COSH(0)
    assertThat(r1.get(8)).isEqualTo(2.72); // ROUND(EXP(1),2)
    assertThat(r1.get(9)).isEqualTo(2); // FLOOR(EXP(1))
    assertThat(r1.get(10)).isEqualTo(5); // GREATEST(1,5,3,4)
    assertThat(r1.get(11)).isEqualTo(1); // LEAST(1,5,3,4)
    assertThat(r1.get(12)).isEqualTo(2.31); // ROUND(LOG(word_count, 2.71), 2)
    assertThat(r1.get(13)).isEqualTo(1.0); // ROUND(LOG10(word_count), 2)
    assertThat(r1.get(14)).isEqualTo(100.0); // POW(word_count, 2)
    assertThat(r1.get(16)).isEqualTo(0.0); // SIN(0)
    assertThat(r1.get(17)).isEqualTo(0.0); // SINH(0)
    assertThat(r1.get(18)).isEqualTo(3.16); // ROUND(SQRT(word_count), 2)
    assertThat(r1.get(19)).isEqualTo(0.0); // TAN(0)
    assertThat(r1.get(20)).isEqualTo(0.0); // TANH(0)
    assertThat(r1.get(21)).isEqualTo(false); // ISNAN(word_count)
    assertThat(r1.get(22)).isEqualTo(1.0); // SIGNUM(word_count)
  }

  @Test
  public void testMiscellaneousExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df.createOrReplaceTempView("shakespeare");
    df =
        df.selectExpr(
                "word",
                "word_count AS WordCount",
                "CAST(word_count as string) AS cast",
                "SHIFTLEFT(word_count, 1) AS ShiftLeft",
                "SHIFTRIGHT(word_count, 1) AS ShiftRight",
                "CASE WHEN word_count > 10 THEN 'frequent' WHEN word_count <= 10 AND word_count > 4 THEN 'normal' ELSE 'rare' END AS WordFrequency",
                "(SELECT MAX(word_count) from shakespeare) as MaxWordCount",
                "(SELECT MAX(word_count) from shakespeare WHERE word IN ('glass', 'augurs')) as MaxWordCountInWords",
                "COALESCE(NULL, NULL, NULL, word, NULL, 'Push', 'Down') as Coalesce",
                "IF(word_count = 10 and word = 'glass', 'working', 'not working') AS IfCondition")
            .where("word_count = 10 and word = 'glass'")
            .orderBy("word_count");

    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(10); // word_count
    assertThat(r1.get(2)).isEqualTo("10"); // word_count
    assertThat(r1.get(3)).isEqualTo(20); // SHIFTLEFT(word_count, 1)
    assertThat(r1.get(4)).isEqualTo(5); // SHIFTRIGHT(word_count, 1)
    assertThat(r1.get(5)).isEqualTo("normal"); // CASE WHEN
    assertThat(r1.get(6)).isEqualTo(995); // SCALAR SUBQUERY
    assertThat(r1.get(7)).isEqualTo(10); // SCALAR SUBQUERY WITH IN
    assertThat(r1.get(8)).isEqualTo("glass"); // COALESCE
    assertThat(r1.get(9)).isEqualTo("working"); // IF CONDITION
  }
}
