/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.connector.common.IntegrationTestUtils;
import com.google.cloud.bigquery.connector.common.TestConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SparkBigQueryConnectorIntegrationTest {


    private static String TEMPORARY_GCS_BUCKET = "davidrab-sandbox";
    private static BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
    private static String LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects";
    private static String SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare";
    private static long SHAKESPEARE_TABLE_NUM_ROWS = 164656L;
    private static StructType SHAKESPEARE_TABLE_SCHEMA = new StructType(new StructField[]{
            new StructField("word", DataTypes.StringType, false, metadata("description",
                    "A single unique word (where whitespace is the delimiter) extracted from a corpus.")),
            new StructField("word_count", DataTypes.LongType, false, metadata("description",
                    "The number of times this word appears in this corpus.")),
            new StructField("corpus", DataTypes.StringType, false, metadata("description",
                    "The work from which this word was extracted.")),
            new StructField("corpus_date", DataTypes.LongType, false, metadata("description",
                    "The year in which this corpus was published."))});

    private static String LARGE_TABLE = "bigquery-public-data.samples.natality";
    private static String LARGE_TABLE_FIELD = "is_male";
    private static long LARGE_TABLE_NUM_ROWS = 33271914L;
    private static String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
    private static String ALL_TYPES_TABLE_NAME = "all_types";

    static SparkSession spark;
    static String testDataset;
    static String testTable;
    static Dataset<Row> allTypesTable;

    @BeforeClass
    public static void init() {
        spark = SparkSession.builder()
                .appName("spark-bigquery test")
                .master("local")
                .getOrCreate();
        testDataset = String.format("spark_bigquery_it_%s", System.currentTimeMillis());
        IntegrationTestUtils.createDataset(testDataset);
        IntegrationTestUtils.runQuery(
                TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(testDataset, ALL_TYPES_TABLE_NAME));
        allTypesTable = spark.read().format("bigquery")
                .option("dataset", testDataset)
                .option("table", ALL_TYPES_TABLE_NAME)
                .load();
    }

    @AfterClass
    public static void close() {
        IntegrationTestUtils.deleteDatasetAndTables(testDataset);
        IntegrationTestUtils.deleteDatasetAndTables(testDataset);
        spark.close();
    }


    private static Metadata metadata(String key, String value) {
        return new MetadataBuilder().putString(key, value).build();
    }

}
