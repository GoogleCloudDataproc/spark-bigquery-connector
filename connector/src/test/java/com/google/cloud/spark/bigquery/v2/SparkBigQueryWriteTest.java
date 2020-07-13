package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public class SparkBigQueryWriteTest {

    public static final Logger logger = LogManager.getLogger("com.google.cloud.spark.bigquery");

    public static final String PROJECT = "google.com:hadoop-cloud-dev";
    public static final String DATASET = "ymed_"+RemoteBigQueryHelper.generateDatasetName();
    public static final String TABLE = "testTable";
    public static final String DESCRIPTION = "Spark BigQuery connector write session tests.";

    public static final String TEST_DATA_FRAME = /*"google.com:hadoop-cloud-dev:ymed.Titanic"*/
            /*"bigquery-public-data:bbc_news.fulltext"*/
            /*"bigquery-public-data:fda_drug.drug_enforcement"*/
            /*"bigquery-public-data:san_francisco_bikeshare.bikeshare_station_info"*/
            /*"bigquery-public-data:austin_incidents.incidents_2016"*/
            "google.com:hadoop-cloud-dev:ymed.all_types";

    public static SparkSession spark;
    public static BigQuery bigquery;
    public static Dataset<Row> df;

    @BeforeClass
    public static void init() {
        logger.setLevel(Level.DEBUG);
        spark = SparkSession
                .builder()
                .appName("Application Name")
                .config("some-config", "some-value")
                .master("local[*]")
                .getOrCreate();
        df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", TEST_DATA_FRAME)
                .load().drop("ts").drop("dt").toDF(); // TODO: when timestamps are supported externally in Vortex, delete ".drop" operations.
        bigquery = BigQueryOptions.getDefaultInstance().getService();
        DatasetInfo datasetInfo =
                DatasetInfo.newBuilder(/* datasetId = */ DATASET).setDescription(DESCRIPTION).build();
        bigquery.create(datasetInfo);
        logger.info("Created test dataset: " + DATASET);
    }

    @AfterClass
    public static void close() {
        if (bigquery != null) {
            RemoteBigQueryHelper.forceDelete(bigquery, DATASET);
            logger.info("Deleted test dataset: " + DATASET);
        }
    }

    @Test
    public void testSparkBigQueryWrite() {
        df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", TABLE)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> actual = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", TABLE)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        assertThat(actual.schema()).isEqualTo(df.schema());

        long intersectionRowCount = actual.intersectAll(df).count();
        assertThat(intersectionRowCount == actual.count() && intersectionRowCount == df.count());
    }
}
