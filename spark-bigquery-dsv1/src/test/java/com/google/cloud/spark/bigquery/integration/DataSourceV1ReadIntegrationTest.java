package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import org.apache.spark.bigquery.BigNumeric;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourceV1ReadIntegrationTest extends ReadIntegrationTestBase {

  static IntegrationTestContext ctx;

  public DataSourceV1ReadIntegrationTest() {
    super(ctx);
  }

  @BeforeClass
  public static void initialize() {
    ctx = IntegrationTestUtils.initialize(DataSourceV1ReadIntegrationTest.class, true);
  }

  @AfterClass
  public static void clean() {
    IntegrationTestUtils.clean(ctx);
  }

  // @TODO Move to suport class once DSv2 supports all types
  @Test
  public void testReadDataTypes() {
    Dataset<Row> allTypesTable = readAllTypesTable();
    Row expectedValues = spark.range(1)
        .select(TestConstants.ALL_TYPES_TABLE_COLS.stream().toArray(Column[]::new)).head();
    Row row = allTypesTable.head();

    for (int i = 0; i < expectedValues.length(); i++) {
      if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
        for (int j = 0; j < 2; j++) {
          BigNumeric bigNumericValue = (BigNumeric) (((GenericRowWithSchema) row.get(i)).get(j));

          String bigNumericString = bigNumericValue.getNumber().toPlainString();

          String expectedBigNumericString =
              (((GenericRowWithSchema) row.get(i)).get(j)).toString();

          assertThat(bigNumericString).isEqualTo(expectedBigNumericString);
          ;
        }
      } else {
        Object value = row.get(i);
        assertThat(value).isEqualTo(expectedValues.get(i));
      }
    }
  }

  // DSv2 does not support Avro
  @Test
  public void testOrAcrossColumnsAndFormats() {
    List<Row> avroResults = spark.read().format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("filter", "word_count = 1 OR corpus_date = 0")
        .option("readDataFormat", "AVRO")
        .load()
        .collectAsList();

    List<Row> arrowResults = spark.read().format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", "ARROW")
        .load()
        .where("word_count = 1 OR corpus_date = 0")
        .collectAsList();

    assertThat(avroResults).isEqualTo(arrowResults);
  }

}
