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

    public static final String BIGQUERY_PUBLIC_DATA = "bigquery-public-data";
    public static final String MB20_DATASET = "baseball";
    public static final String MB20_TABLE = "games_post_wide";
    public static final String MB20_ID = BIGQUERY_PUBLIC_DATA+":"+MB20_DATASET+"."+MB20_TABLE;
    public static final String MB100_DATASET = "open_images";
    public static final String MB100_TABLE = "annotations_bbox";
    public static final String MB100_ID = BIGQUERY_PUBLIC_DATA+":"+MB100_DATASET+"."+MB100_TABLE; // 156 MB
    public static final String GB3_DATASET = "open_images";
    public static final String GB3_TABLE = "images";
    public static final String GB3_ID = BIGQUERY_PUBLIC_DATA+":"+GB3_DATASET+"."+GB3_TABLE; // 3.56 GB
    public static final String GB20_DATASET = "samples";
    public static final String GB20_TABLE = "natality";
    public static final String GB20_ID = BIGQUERY_PUBLIC_DATA+":"+GB20_DATASET+"."+GB20_TABLE; // 21 GB
    public static final String GB250_DATASET = "samples";
    public static final String GB250_TABLE = "trigrams";
    public static final String GB250_ID = BIGQUERY_PUBLIC_DATA+":"+GB250_DATASET+"."+GB250_TABLE; //256 GB

    public static SparkSession spark;
    public static BigQuery bigquery;

    public static Dataset<Row> allTypesDf;
    public static Dataset<Row> MB20Df;
    public static Dataset<Row> MB100Df;
    public static Dataset<Row> GB3Df;
    public static Dataset<Row> GB20Df;
    public static Dataset<Row> GB250Df;


    @BeforeClass
    public static void init() throws Exception {
        logger.setLevel(Level.DEBUG);
        spark = SparkSession
                .builder()
                .appName("Application Name")
                .config("some-config", "some-value")
                .master("local[*]")
                .getOrCreate();
        allTypesDf = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", ALL_TYPES_TABLE)
                .load().drop("timestamp").toDF(); // TODO: when timestamps are supported externally in Vortex, delete ".drop" operations.
        MB20Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", MB20_ID)
                .load().drop("startTime").drop("createdAt").drop("updatedAt")/*.coalesce(20)*/.toDF();
        MB100Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", MB100_ID)
                .load()/*.coalesce(20).toDF()*/;
        GB3Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB3_ID)
                .load()/*.coalesce(20).toDF()*/;
        GB20Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB20_ID)
                .load()/*.coalesce(20).toDF()*/;
        GB250Df = spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", GB250_ID)
                .load()/*.coalesce(20).toDF()*/;

        bigquery = BigQueryOptions.getDefaultInstance().getService();
        DatasetInfo datasetInfo =
                DatasetInfo.newBuilder(/* datasetId = */ DATASET).setDescription(DESCRIPTION).build();
        bigquery.create(datasetInfo);
        logger.info("Created test dataset: " + DATASET);
    }

    @AfterClass
    public static void close() throws Exception {
        if (bigquery != null) {
            RemoteBigQueryHelper.forceDelete(bigquery, DATASET);
            logger.info("Deleted test dataset: " + DATASET);
        }
    }

    @Test
    public void testSparkBigQueryWriteAllTypes() throws Exception {
        String writeTo = "all_types";

        Dataset<Row> expectedDF = allTypesDf;

        expectedDF.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> actualDF = spark.read()
                .format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .load();

        assertThat(actualDF.schema()).isEqualTo(expectedDF.schema());

        Dataset<Row> intersection = actualDF.intersectAll(expectedDF);
        assertThat(intersection.count() == actualDF.count() && intersection.count() == expectedDF.count());
    }

    @Test
    public void testSparkBigQueryWriterAbort() throws Exception {

    }

    @Test
    public void testSparkBigQueryWrite20MB() throws Exception {
        String writeTo = "20MB";

        MB20Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        // TODO: simple num bytes print line

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, MB20_DATASET, MB20_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite100MB() throws Exception {
        String writeTo = "100MB";

        MB100Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, MB100_DATASET, MB100_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite3GB() throws Exception {
        String writeTo = "3GB";

        GB3Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, GB3_DATASET, GB3_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite20GB() throws Exception {
        String writeTo = "20GB";

        GB20Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, GB20_DATASET, GB20_TABLE)).getNumBytes());
    }

    @Test
    public void testSparkBigQueryWrite250GB() throws Exception {
        String writeTo = "250GB";

        GB250Df.write().format("com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2")
                .option("table", writeTo)
                .option("dataset", DATASET)
                .option("project", PROJECT)
                .mode(SaveMode.Overwrite)
                .save();

        assertThat(bigquery.getTable(TableId.of(PROJECT, DATASET, writeTo)).getNumBytes()
                == bigquery.getTable(TableId.of(BIGQUERY_PUBLIC_DATA, GB250_DATASET, GB250_TABLE)).getNumBytes());
    }
}
