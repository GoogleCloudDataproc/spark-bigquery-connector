package com.google.cloud.spark.bigquery.it;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.spark.bigquery.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static java.time.Duration.ofSeconds;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.fail;

public class SparkBigQueryEndToEndITTest {/*
    val filterData = Table(
            ("condition", "elements"),
    ("word_count == 4", Seq("'A", "'But", "'Faith")),
            ("word_count > 3", Seq("'", "''Tis", "'A")),
            ("word_count >= 2", Seq("'", "''Lo", "''O")),
            ("word_count < 3", Seq("''All", "''Among", "''And")),
            ("word_count <= 5", Seq("'", "''All", "''Among")),
            ("word_count in(8, 9)", Seq("'", "'Faith", "'Tis")),
            ("word_count is null", Seq()),
            ("word_count is not null", Seq("'", "''All", "''Among")),
            ("word_count == 4 and corpus == 'twelfthnight'", Seq("'Thou", "'em", "Art")),
            ("word_count == 4 or corpus > 'twelfthnight'", Seq("'", "''Tis", "''twas")),
            ("not word_count in(8, 9)", Seq("'", "''All", "''Among")),
            ("corpus like 'king%'", Seq("'", "'A", "'Affectionate")),
            ("corpus like '%kinghenryiv'", Seq("'", "'And", "'Anon")),
            ("corpus like '%king%'", Seq("'", "'A", "'Affectionate"))
            )
    public final String temporaryGcsBucket = "davidrab-sandbox";
    public final BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
    private final String LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects";
    private final String SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare";
    private final long SHAKESPEARE_TABLE_NUM_ROWS = 164656L;
    private final StructType SHAKESPEARE_TABLE_SCHEMA = new StructType()
            .add(new StructField("word", StringType, false, metadata("description",
                    "A single unique word (where whitespace is the delimiter) extracted from a corpus.")))
            .add(new StructField("word_count", LongType, false, metadata("description",
                    "The number of times this word appears in this corpus.")))
            .add(new StructField("corpus", StringType, false, metadata("description",
                    "The work from which this word was extracted.")))
            .add(new StructField("corpus_date", LongType, nullable = false, metadata("description",
                    "The year in which this corpus was published.")));
    private final String LARGE_TABLE = "bigquery-public-data.samples.natality";
    private final String LARGE_TABLE_FIELD = "is_male";
    private final long LARGE_TABLE_NUM_ROWS = 33271914L;
    private final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
    private final String ALL_TYPES_TABLE_NAME = "all_types";
    private static SparkSession spark;
    private static String testDataset;
    private static String testTable;

    private Metadata metadata(String key, : String value){
        return new MetadataBuilder().putString(key, value).build();
    }

    private Metadata metadata(Map<String, String> map) {
        MetadataBuilder metadata = new MetadataBuilder();
        for(String key : map.keySet()) {
            metadata.putString(key, map.get(key));
        }
        return metadata.build();
    }

    @Before
    public static void beforeEach() {
        // have a fresh table for each test
        testTable = "test_"+System.nanoTime();
    }

    /*
    @Test
    public void testImplicitReadMethod() {
    import com.google.cloud.spark.bigquery._
        spark.read().bigquery(SHAKESPEARE_TABLE)
    }
     */
/*
    @Test
    public void testExplicitFormat() throws Exception {
        testShakespeare(spark.read().format("com.google.cloud.spark.bigquery")
                .option("table", SHAKESPEARE_TABLE)
                .load());
    }

    @Test
    public void testShortFormat() throws Exception {
        testShakespeare(spark.read().format("bigquery").option("table", SHAKESPEARE_TABLE).load());
    }

    @Test
    public void testSimplifiedApi() throws Exception {
        testShakespeare(spark.read().format("bigquery").load(SHAKESPEARE_TABLE));
    }

    @Test
    public void testDataSourceV2() throws Exception {
        testShakespeare(spark.read().format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
                .option("table", SHAKESPEARE_TABLE).load());
    }

    @Test
    public void testWithReadInSeveralFormats() throws Exception {
        for (String dataFormat : new String[]{"avro", "arrow"}) {
            for(String dataSourceFormat : new String[]{"bigquery", "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2"}) {
                testsWithReadInFormat(dataSourceFormat, dataFormat);
            }
        }
    }

    @BeforeClass
    public static void beforeAll() throws Exception {
        spark = TestUtils.getOrCreateSparkSession();
        testDataset = "spark_bigquery_it_"+System.currentTimeMillis();
        IntegrationTestUtils.createDataset(testDataset);
        IntegrationTestUtils.runQuery(
                TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(s"$testDataset.$ALL_TYPES_TABLE_NAME"))
    }

    test("test filters") {
    import com.google.cloud.spark.bigquery._
        val sparkImportVal = spark
    import sparkImportVal.implicits._
        forAll(filterData) { (condition, expectedElements) =>
            val df = spark.read.bigquery(SHAKESPEARE_TABLE)
            assert(SHAKESPEARE_TABLE_SCHEMA == df.schema)
            assert(SHAKESPEARE_TABLE_NUM_ROWS == df.count)
            val firstWords = df.select("word")
                    .where(condition)
                    .distinct
                    .as[String]
                    .sort("word")
                    .take(3)
            firstWords should contain theSameElementsInOrderAs expectedElements
        }
    }

    public static void testsWithReadInFormat(String dataSourceFormat, String dataFormat) throws Exception {
        try {
            Row row = spark.read().format(dataSourceFormat)
                    .option("table", SHAKESPEARE_TABLE)
                    .option("readDataFormat", dataFormat).load()
                    .select("word_count", "word").head();
            assertThat(row.get(0)).isInstanceOf(Long.class);
            assertThat(row.get(1)).isInstanceOf(String.class);
        }
        catch (Exception e){
            fail("out of order columns. DataSource %s. Data Format %s"
                    .format(dataSourceFormat, dataFormat));
        }

        try {
            Dataset<Row> allTypesTable = readAllTypesTable("bigquery");
            writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro");

            Dataset<Row> df = spark.read().format("bigquery")
                    .option("dataset", testDataset)
                    .option("table", testTable)
                    .option("readDataFormat", "arrow")
                    .load().cache();

            assertThat(df.head()).isEqualTo(allTypesTable.head());

            // read from cache
            assertThat(df.head()).isEqualTo(allTypesTable.head());
            assertThat(df.schema()).isEqualTo(allTypesTable.schema());
        }
        catch (Exception e) {
            fail("cache data frame in DataSource %s. Data Format %s".format(dataSourceFormat, dataFormat))
        }

        try {
            Dataset<Row> df = spark.read().format("com.google.cloud.spark.bigquery")
                    .option("table", LARGE_TABLE)
                    .option("parallelism", "5")
                    .option("readDataFormat", dataFormat)
                    .load();
            assertThat(5 == df.rdd().getNumPartitions());
        }
        catch (Exception e) {
            fail("number of partitions. DataSource %s. Data Format %s"
                    .format(dataSourceFormat, dataFormat));
        }

        try {
            Dataset<Row> df = spark.read().format("com.google.cloud.spark.bigquery")
                    .option("table", LARGE_TABLE)
                    .option("readDataFormat", dataFormat)
                    .load();
            assertThat(df.rdd().getNumPartitions() == 35)
        }
        catch (Exception e) {
            fail("default number of partitions. DataSource %s. Data Format %s"
                    .format(dataSourceFormat, dataFormat));
        }

        try {
            assertTimeout(ofSeconds(120) -> {
                // Select first partition
                val df = spark.read
                        .option("parallelism", 5)
                        .option("readDataFormat", dataFormat)
                        .option("filter", "year > 2000")
                        .bigquery(LARGE_TABLE)
                        .select(LARGE_TABLE_FIELD) // minimize payload
                val sizeOfFirstPartition = df.rdd.mapPartitionsWithIndex {
                    case (_, it) => Iterator(it.size)
                }.collect().head

                // Since we are only reading from a single stream, we can expect to get
                // at least as many rows
                // in that stream as a perfectly uniform distribution would command.
                // Note that the assertion
                // is on a range of rows because rows are assigned to streams on the
                // server-side in
                // indivisible units of many rows.

                val numRowsLowerBound = LARGE_TABLE_NUM_ROWS / df.rdd.getNumPartitions
                assert(numRowsLowerBound <= sizeOfFirstPartition &&
                        sizeOfFirstPartition < (numRowsLowerBound * 1.1).toInt)
            });
        }
        catch (Exception e) {
            fail("balanced partitions. DataSource %s. Data Format %s"
                    .format(dataSourceFormat, dataFormat));
        }

        test("test optimized count(*). DataSource %s. Data Format %s"
                .format(dataSourceFormat, dataFormat)) {
            DirectBigQueryRelation.emptyRowRDDsCreated = 0
            val oldMethodCount = spark.read.format(dataSourceFormat)
                    .option("table", "bigquery-public-data.samples.shakespeare")
                    .option("optimizedEmptyProjection", "false")
                    .option("readDataFormat", dataFormat)
                    .load()
                    .select("corpus_date")
                    .where("corpus_date > 0")
                    .count()

            assert(DirectBigQueryRelation.emptyRowRDDsCreated == 0)

            assertResult(oldMethodCount) {
                spark.read.format(dataSourceFormat)
                        .option("table", "bigquery-public-data.samples.shakespeare")
                        .option("readDataFormat", dataFormat)
                        .load()
                        .where("corpus_date > 0")
                        .count()
            }

            if("bigquery" == dataSourceFormat) {
                assert(DirectBigQueryRelation.emptyRowRDDsCreated == 1)
            }
        }

        test("test optimized count(*) with filter. DataSource %s. Data Format %s"
                .format(dataSourceFormat, dataFormat)) {
            DirectBigQueryRelation.emptyRowRDDsCreated = 0
            val oldMethodCount = spark.read.format(dataSourceFormat)
                    .option("table", "bigquery-public-data.samples.shakespeare")
                    .option("optimizedEmptyProjection", "false")
                    .option("readDataFormat", dataFormat)
                    .load()
                    .select("corpus_date")
                    .count()

            assert(DirectBigQueryRelation.emptyRowRDDsCreated == 0)

            assertResult(oldMethodCount) {
                spark.read.format(dataSourceFormat)
                        .option("table", "bigquery-public-data.samples.shakespeare")
                        .option("readDataFormat", dataFormat)
                        .load()
                        .count()
            }
            if("bigquery" == dataSourceFormat) {
                assert(DirectBigQueryRelation.emptyRowRDDsCreated == 1)
            }
        }

        test("keeping filters behaviour. DataSource %s. Data Format %s"
                .format(dataSourceFormat, dataFormat)) {
            val newBehaviourWords = extractWords(
                    spark.read.format(dataSourceFormat)
                            .option("table", "bigquery-public-data.samples.shakespeare")
                            .option("filter", "length(word) = 1")
                            .option("combinePushedDownFilters", "true")
                            .option("readDataFormat", dataFormat)
                            .load())

            val oldBehaviourWords = extractWords(
                    spark.read.format(dataSourceFormat)
                            .option("table", "bigquery-public-data.samples.shakespeare")
                            .option("filter", "length(word) = 1")
                            .option("combinePushedDownFilters", "false")
                            .option("readDataFormat", dataFormat)
                            .load())

            newBehaviourWords should equal (oldBehaviourWords)
        }
    }

    def readAllTypesTable(dataSourceFormat: String): DataFrame =
            spark.read.format(dataSourceFormat)
            .option("dataset", testDataset)
      .option("table", ALL_TYPES_TABLE_NAME)
      .load()


    Seq("bigquery", "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
    .foreach(testsWithDataSource)

    def testsWithDataSource(dataSourceFormat: String) {

        test("OR across columns with Arrow. DataSource %s".format(dataSourceFormat)) {

            val avroResults = spark.read.format("bigquery")
                    .option("table", "bigquery-public-data.samples.shakespeare")
                    .option("filter", "word_count = 1 OR corpus_date = 0")
                    .option("readDataFormat", "AVRO")
                    .load().collect()

            val arrowResults = spark.read.format("bigquery")
                    .option("table", "bigquery-public-data.samples.shakespeare")
                    .option("readDataFormat", "ARROW")
                    .load().where("word_count = 1 OR corpus_date = 0")
                    .collect()

            avroResults should equal(arrowResults)
        }

        // Disabling the test until the merge the master
        // TODO: enable it
    /*
    test("Count with filters - Arrow. DataSource %s".format(dataSourceFormat)) {

      val countResults = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", "ARROW")
        .load().where("word_count = 1 OR corpus_date = 0")
        .count()

      val countAfterCollect = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", "ARROW")
        .load().where("word_count = 1 OR corpus_date = 0")
        .collect().size

      countResults should equal(countAfterCollect)
    }
    */
/*
        test("read data types. DataSource %s".format(dataSourceFormat)) {
            val allTypesTable = readAllTypesTable(dataSourceFormat)
            val expectedRow = spark.range(1).select(TestConstants.ALL_TYPES_TABLE_COLS: _*).head.toSeq
            val row = allTypesTable.head.toSeq
            row should contain theSameElementsInOrderAs expectedRow
        }


        test("known size in bytes. DataSource %s".format(dataSourceFormat)) {
            val allTypesTable = readAllTypesTable(dataSourceFormat)
            val actualTableSize = allTypesTable.queryExecution.analyzed.stats.sizeInBytes
            assert(actualTableSize == ALL_TYPES_TABLE_SIZE)
        }

        test("known schema. DataSource %s".format(dataSourceFormat)) {
            val allTypesTable = readAllTypesTable(dataSourceFormat)
            assert(allTypesTable.schema == ALL_TYPES_TABLE_SCHEMA)
        }

        test("user defined schema. DataSource %s".format(dataSourceFormat)) {
            // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
            val expectedSchema = StructType(Seq(StructField("whatever", ByteType)))
            val table = spark.read.schema(expectedSchema)
                    .format(dataSourceFormat)
                    .option("table", SHAKESPEARE_TABLE)
                    .load()
            assert(expectedSchema == table.schema)
        }

        test("non-existent schema. DataSource %s".format(dataSourceFormat)) {
            assertThrows[RuntimeException] {
                spark.read.format(dataSourceFormat).option("table", NON_EXISTENT_TABLE).load()
            }
        }

        test("head does not time out and OOM. DataSource %s".format(dataSourceFormat)) {
            failAfter(10 seconds) {
                spark.read.format(dataSourceFormat)
                        .option("table", LARGE_TABLE)
                        .load()
                        .select(LARGE_TABLE_FIELD)
                        .head
            }
        }
    }
    // Write tests. We have four save modes: Append, ErrorIfExists, Ignore and
    // Overwrite. For each there are two behaviours - the table exists or not.
    // See more at http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/sql/SaveMode.html

    override def afterAll: Unit = {
        IntegrationTestUtils.deleteDatasetAndTables(testDataset)
        spark.stop()
    }

    /** Generate a test to verify that the given DataFrame is equal to a known result. */
/*
    private void testShakespeare(Dataset<Row> df) {
        assertThat(SHAKESPEARE_TABLE_SCHEMA).isEqualTo(df.schema());
        assertThat(SHAKESPEARE_TABLE_NUM_ROWS).isEqualTo(df.count());
        Row[] firstWords = df.select("word")
                .where("word >= 'a' AND word not like '%\\'%'")
                .distinct()
                .as("String").sort("word").take(3);

        String[] expected = new String[]{"a", "abaissiez", "abandon"};
        for(int i = 0; i < 3; i++){
            assertThat(firstWords[0].get(0)).isEqualTo(expected[i]);
        }
    }

    private def initialData = spark.createDataFrame(spark.sparkContext.parallelize(
            Seq(Person("Abc", Seq(Friend(10, Seq(Link("www.abc.com"))))),
                    Person("Def", Seq(Friend(12, Seq(Link("www.def.com"))))))))

    private def additonalData = spark.createDataFrame(spark.sparkContext.parallelize(
            Seq(Person("Xyz", Seq(Friend(10, Seq(Link("www.xyz.com"))))),
                    Person("Pqr", Seq(Friend(12, Seq(Link("www.pqr.com"))))))))

    // getNumRows returns BigInteger, and it messes up the matchers
    private def testTableNumberOfRows = bq.getTable(testDataset, testTable).getNumRows.intValue

    private def testPartitionedTableDefinition = bq.getTable(testDataset, testTable + "_partitioned")
            .getDefinition[StandardTableDefinition]()

    private def writeToBigQuery(df: DataFrame, mode: SaveMode, format: String = "parquet") =
            df.write.format("bigquery")
            .mode(mode)
      .option("table", fullTableName)
      .option("temporaryGcsBucket", temporaryGcsBucket)
      .option(SparkBigQueryOptions.IntermediateFormatOption, format)
      .save()

    private def initialDataValuesExist = numberOfRowsWith("Abc") == 1

    private def numberOfRowsWith(name: String) =
            bq.query(QueryJobConfiguration.of(s"select name from $fullTableName where name='$name'"))
            .getTotalRows

    private def fullTableName = s"$testDataset.$testTable"
    private def fullTableNamePartitioned = s"$testDataset.${testTable}_partitioned"

    private def additionalDataValuesExist = numberOfRowsWith("Xyz") == 1

    test("write to bq - append save mode") {
        // initial write
        writeToBigQuery(initialData, SaveMode.Append)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
        // second write
        writeToBigQuery(additonalData, SaveMode.Append)
        testTableNumberOfRows shouldBe 4
        additionalDataValuesExist shouldBe true
    }

    test("write to bq - error if exists save mode") {
        // initial write
        writeToBigQuery(initialData, SaveMode.ErrorIfExists)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
        // second write
        assertThrows[IllegalArgumentException] {
            writeToBigQuery(additonalData, SaveMode.ErrorIfExists)
        }
    }

    test("write to bq - ignore save mode") {
        // initial write
        writeToBigQuery(initialData, SaveMode.Ignore)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
        // second write
        writeToBigQuery(additonalData, SaveMode.Ignore)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
        additionalDataValuesExist shouldBe false
    }

    test("write to bq - overwrite save mode") {
        // initial write
        writeToBigQuery(initialData, SaveMode.Overwrite)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
        // second write
        writeToBigQuery(additonalData, SaveMode.Overwrite)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe false
        additionalDataValuesExist shouldBe true
    }

    test("write to bq - orc format") {
        // required by ORC
        spark.conf.set("spark.sql.orc.impl", "native")
        writeToBigQuery(initialData, SaveMode.ErrorIfExists, "orc")
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
    }

    test("write to bq - avro format") {
        writeToBigQuery(initialData, SaveMode.ErrorIfExists, "avro")
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
    }

    test("write to bq - parquet format") {
        writeToBigQuery(initialData, SaveMode.ErrorIfExists, "parquet")
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
    }

    test("write to bq - simplified api") {
        initialData.write.format("bigquery")
                .option("temporaryGcsBucket", temporaryGcsBucket)
                .save(fullTableName)
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
    }

    test("write to bq - unsupported format") {
        assertThrows[IllegalArgumentException] {
            writeToBigQuery(initialData, SaveMode.ErrorIfExists, "something else")
        }
    }

    test("write all types to bq - avro format") {
        val allTypesTable = readAllTypesTable("bigquery")
        writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro")

        val df = spark.read.format("bigquery")
                .option("dataset", testDataset)
                .option("table", testTable)
                .load()

        assert(df.head() == allTypesTable.head())
        assert(df.schema == allTypesTable.schema)
    }

    test("query materialized view") {
        var df = spark.read.format("bigquery")
                .option("table", "bigquery-public-data:ethereum_blockchain.live_logs")
                .option("viewsEnabled", "true")
                .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
                .option("viewMaterializationDataset", testDataset)
                .load()
    }

    test("write to bq - adding the settings to spark.conf" ) {
        spark.conf.set("temporaryGcsBucket", temporaryGcsBucket)
        val df = initialData
        df.write.format("bigquery")
                .option("table", fullTableName)
                .save()
        testTableNumberOfRows shouldBe 2
        initialDataValuesExist shouldBe true
    }

    test ("write to bq - partitioned and clustered table") {
        val df = spark.read.format("com.google.cloud.spark.bigquery")
                .option("table", LIBRARIES_PROJECTS_TABLE)
                .load()
                .where("platform = 'Sublime'")

        df.write.format("bigquery")
                .option("table", fullTableNamePartitioned)
                .option("temporaryGcsBucket", temporaryGcsBucket)
                .option("partitionField", "created_timestamp")
                .option("clusteredFields", "platform")
                .mode(SaveMode.Overwrite)
                .save()

        val tableDefinition = testPartitionedTableDefinition
        tableDefinition.getTimePartitioning.getField shouldBe "created_timestamp"
        tableDefinition.getClustering.getFields should contain ("platform")
    }

    def extractWords(df: DataFrame): Set[String] = {
        df.select("word")
                .where("corpus_date = 0")
                .collect()
                .map(_.getString(0))
                .toSet
    }
}

case class Person(name: String, friends: Seq[Friend])
        case class Friend(age: Int, links: Seq[Link])
        case class Link(uri: String)
}
       */ }