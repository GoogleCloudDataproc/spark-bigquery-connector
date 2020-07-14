package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public class SparkBigQueryWriteTest {

    public static final Logger logger = LogManager.getLogger("com.google.cloud.spark.bigquery");

    public static final String PROJECT = ServiceOptions.getDefaultProjectId();
    public static final String DATASET = "spark_bigquery_it_"+RemoteBigQueryHelper.generateDatasetName(); // TODO: make own random number generator.
    public static final String TABLE = "testTable";
    public static final String DESCRIPTION = "Spark BigQuery connector write session tests.";

    public static final String ALL_TYPES_TABLE = "google.com:hadoop-cloud-dev:ymed.all_types";  // TODO: move this table to a different project

    public static final String MB20_TABLE = "bigquery-public-data:baseball.games_post_wide";
    public static final String MB100_TABLE = "bigquery-public-data:open_images.annotations_bbox"; // 156 MB
    public static final String GB3_TABLE = "bigquery-public-data:open_images.images"; // 3.56 GB
    public static final String GB20_TABLE = "bigquery-public-data:samples.natality"; // 21 GB
    public static final String GB250_TABLE = "bigquery-public-data:samples.trigrams"; //256 GB

    public static SparkSession spark;
    public static BigQuery bigquery;

    public static Dataset<Row> allTypesDf;
    public static Dataset<Row> MB20Df;
    public static Dataset<Row> MB100Df;
    public static Dataset<Row> GB3Df;
    public static Dataset<Row> GB20Df;
    public static Dataset<Row> GB250Df;


    @BeforeClass
    public static void init() {
        logger.setLevel(Level.DEBUG);
        spark = SparkSession
                .builder()
                .appName("Application Name")
                .config("some-config", "some-value")
                .master("local[*]")
                .getOrCreate();
        allTypesDf = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", ALL_TYPES_TABLE)
                .load().drop("ts").drop("dt").toDF(); // TODO: when timestamps are supported externally in Vortex, delete ".drop" operations.
        MB20Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", MB20_TABLE)
                .load().drop("startTime").drop("createdAt").drop("updatedAt").repartition(20).toDF();
        MB100Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", MB100_TABLE)
                .load();
        GB3Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB3_TABLE)
                .load();
        GB20Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB20_TABLE)
                .load();
        GB250Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB250_TABLE)
                .load();

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
    public void testSparkBigQueryWriteAllTypes() {
        String writeTo = "all_types";

        allTypesDf.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> actual = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        assertThat(actual.schema()).isEqualTo(allTypesDf.schema());

        Dataset intersection = actual.intersectAll(allTypesDf);
        assertThat(intersection.count() == actual.count() && intersection.count() == allTypesDf.count());
    }

    @Test
    public void testSparkBigQueryWriterAbort() {

    }

    @Test
    public void testSparkBigQueryWrite20MB() {
        String writeTo = "20MB";

        MB20Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes() != 0);
    }

    @Test
    public void testSparkBigQueryWrite100MB() {
        String writeTo = "100MB";

        MB100Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes() != 0);
    }

    @Test
    public void testSparkBigQueryWrite3GB() {
        String writeTo = "3GB";

        GB3Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes() != 0);
    }

    @Test
    public void testSparkBigQueryWrite20GB() {
        String writeTo = "20GB";

        GB20Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes() != 0);
    }

    @Test
    public void testSparkBigQueryWrite250GB() {
        String writeTo = "250GB";

        GB250Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes() != 0);
    }
}
