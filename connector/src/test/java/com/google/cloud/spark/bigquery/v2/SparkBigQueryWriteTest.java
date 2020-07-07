package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
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

public class SparkBigQueryWriteTest {

    public static final Logger logger = LogManager.getLogger("com.google.cloud.spark.bigquery");

    public static final String DATASET = RemoteBigQueryHelper.generateDatasetName();
    private static final String DESCRIPTION = "Spark BigQuery connector write session tests.";

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
                .option("table", "google.com:hadoop-cloud-dev:ymed.PittsburghPools")
                .load();
        RemoteBigQueryHelper bigqueryHelper = RemoteBigQueryHelper.create();
        bigquery = bigqueryHelper.getOptions().getService();
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
                .option("table", "testtable")
                .option("database", DATASET)
                .mode(SaveMode.Overwrite)
                .save();
    }
}
