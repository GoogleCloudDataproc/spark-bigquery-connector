# Apache Spark SQL connector for Google BigQuery

<!--- TODO(#2): split out into more documents. -->

The connector supports reading [Google BigQuery](https://cloud.google.com/bigquery/) tables into Spark's DataFrames, and writing DataFrames back into BigQuery.
This is done by using the [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to communicate with BigQuery.

## Unreleased Changes

This Readme may include documentation for changes that haven't been released yet.  The latest release's documentation and source code are found here.

https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/master/README.md

## BigQuery Storage API
The [Storage API](https://cloud.google.com/bigquery/docs/reference/storage) streams data in parallel directly from BigQuery via gRPC without using Google Cloud Storage as an intermediary.

It has a number of advantages over using the previous export-based read flow that should generally lead to better read performance:

### Direct Streaming

It does not leave any temporary files in Google Cloud Storage. Rows are read directly from BigQuery servers using the Arrow or Avro wire formats.

### Filtering

The new API allows column and predicate filtering to only read the data you are interested in.

#### Column Filtering

Since BigQuery is [backed by a columnar datastore](https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format), it can efficiently stream data without reading all columns.

#### Predicate Filtering

The Storage API supports arbitrary pushdown of predicate filters. Connector version 0.8.0-beta and above support pushdown of arbitrary filters to Bigquery.

There is a known issue in Spark that does not allow pushdown of filters on nested fields. For example - filters like `address.city = "Sunnyvale"` will not get pushdown to Bigquery.

### Dynamic Sharding

The API rebalances records between readers until they all complete. This means that all Map phases will finish nearly concurrently. See this blog article on [how dynamic sharding is similarly used in Google Cloud Dataflow](https://cloud.google.com/blog/products/gcp/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow).

See [Configuring Partitioning](#configuring-partitioning) for more details.

## Requirements

### Enable the BigQuery Storage API

Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

### Create a Google Cloud Dataproc cluster (Optional)

If you do not have an Apache Spark environment you can create a Cloud Dataproc cluster with pre-configured auth. The following examples assume you are using Cloud Dataproc, but you can use `spark-submit` on any cluster.

Any Dataproc cluster using the API needs the 'bigquery'  or 'cloud-platform' scopes. Dataproc clusters have the 'bigquery' scope by default, so most clusters in enabled projects should work by default e.g.

```
MY_CLUSTER=...
gcloud dataproc clusters create "$MY_CLUSTER"
```

## Downloading and Using the Connector

The latest version of the connector is publicly available in the following links:

| version    | Link                                                                                                                                                                                                                   |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Spark 3.3  | `gs://spark-lib/bigquery/spark-3.3-bigquery-${next-release-tag}.jar`([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-3.3-bigquery-${next-release-tag}.jar))                                        |
| Spark 3.2  | `gs://spark-lib/bigquery/spark-3.2-bigquery-${next-release-tag}.jar`([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-3.2-bigquery-${next-release-tag}.jar))                                        |
| Spark 3.1  | `gs://spark-lib/bigquery/spark-3.1-bigquery-${next-release-tag}.jar`([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-3.1-bigquery-${next-release-tag}.jar))                                        |
| Spark 2.4  | `gs://spark-lib/bigquery/spark-2.4-bigquery-${next-release-tag}.jar`([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-2.4-bigquery-${next-release-tag}.jar))                                        |
| Scala 2.13 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-${next-release-tag}.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-${next-release-tag}.jar)) |
| Scala 2.12 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-${next-release-tag}.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-${next-release-tag}.jar)) |
| Scala 2.11 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.29.0.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.29.0.jar))                           |

The first four versions are Java based connectors targeting Spark 2.4/3.1/3.2/3.3 of all Scala versions built on the new
Data Source APIs (Data Source API v2) of Spark.

The final two connectors are Scala based connectors, please use the jar relevant to your Spark installation as outlined
below.

### Connector to Spark Compatibility Matrix
| Connector \ Spark                     | 2.3     | 2.4<br>(Scala 2.11) | 2.4<br>(Scala 2.12) | 3.0     | 3.1     | 3.2     | 3.3     |
|---------------------------------------|---------|---------------------|---------------------|---------|---------|---------|---------|
| spark-3.3-bigquery                    |         |                     |                     |         |         |         | &check; |
| spark-3.2-bigquery                    |         |                     |                     |         |         | &check; | &check; |
| spark-3.1-bigquery                    |         |                     |                     |         | &check; | &check; | &check; |
| spark-2.4-bigquery                    |         | &check;             | &check;             |         |         |         |         |
| spark-bigquery-with-dependencies_2.13 |         |                     |                     |         |         | &check; | &check; |
| spark-bigquery-with-dependencies_2.12 |         |                     | &check;             | &check; | &check; | &check; | &check; |
| spark-bigquery-with-dependencies_2.11 | &check; | &check;             |                     |         |         |         |         |

### Connector to Dataproc Image Compatibility Matrix
| Connector \ Dataproc Image            | 1.3     | 1.4     | 1.5     | 2.0     | 2.1     | Serverless<br>Image 1.0 | Serverless<br>Image 2.0 |
|---------------------------------------|---------|---------|---------|---------|---------|-------------------------|-------------------------|
| spark-3.3-bigquery                    |         |         |         |         | &check; | &check;                 | &check;                 |
| spark-3.2-bigquery                    |         |         |         |         | &check; | &check;                 | &check;                 |
| spark-3.1-bigquery                    |         |         |         | &check; | &check; | &check;                 | &check;                 |
| spark-2.4-bigquery                    |         | &check; | &check; |         |         |                         |                         |
| spark-bigquery-with-dependencies_2.13 |         |         |         |         |         |                         | &check;                 |
| spark-bigquery-with-dependencies_2.12 |         |         | &check; | &check; | &check; | &check;                 |                         |
| spark-bigquery-with-dependencies_2.11 | &check; | &check; |         |         |         |                         |                         |

### Maven / Ivy Package Usage
The connector is also available from the
[Maven Central](https://repo1.maven.org/maven2/com/google/cloud/spark/)
repository. It can be used using the `--packages` option or the
`spark.jars.packages` configuration property. Use the following value

| version    | Connector Artifact                                                                 |
|------------|------------------------------------------------------------------------------------|
| Spark 3.3  | `com.google.cloud.spark:spark-3.3-bigquery:${next-release-tag}`                    |
| Spark 3.2  | `com.google.cloud.spark:spark-3.2-bigquery:${next-release-tag}`                    |
| Spark 3.1  | `com.google.cloud.spark:spark-3.1-bigquery:${next-release-tag}`                    |
| Spark 2.4  | `com.google.cloud.spark:spark-2.4-bigquery:${next-release-tag}`                    |
| Scala 2.13 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:${next-release-tag}` |
| Scala 2.12 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:${next-release-tag}` |
| Scala 2.11 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.29.0`              |

## Hello World Example

You can run a simple PySpark wordcount against the API without compilation by running

**Dataproc image 1.5 and above**

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-${next-release-tag}.jar \
  examples/python/shakespeare.py
```

**Dataproc image 1.4 and below**

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.29.0.jar \
  examples/python/shakespeare.py
```

## Example Codelab ##
https://codelabs.developers.google.com/codelabs/pyspark-bigquery

## Usage

The connector uses the cross language [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources):

### Reading data from a BigQuery table

```
df = spark.read \
  .format("bigquery") \
  .load("bigquery-public-data.samples.shakespeare")
```

or the Scala only implicit API:

```
import com.google.cloud.spark.bigquery._
val df = spark.read.bigquery("bigquery-public-data.samples.shakespeare")
```

For more information, see additional code samples in
[Python](examples/python/shakespeare.py),
[Scala](spark-bigquery-dsv1/src/main/scala/com/google/cloud/spark/bigquery/examples/Shakespeare.scala)
and
[Java](spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/examples/JavaShakespeare.java).

### Reading data from a BigQuery query

The connector allows you to run any
[Standard SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
SELECT query on BigQuery and fetch its results directly to a Spark Dataframe.
This is easily done as described in the following code sample:
```
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","<dataset>")

sql = """
  SELECT tag, COUNT(*) c
  FROM (
    SELECT SPLIT(tags, '|') tags
    FROM `bigquery-public-data.stackoverflow.posts_questions` a
    WHERE EXTRACT(YEAR FROM creation_date)>=2014
  ), UNNEST(tags) tag
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
  """
df = spark.read.format("bigquery").load(sql)
df.show()
```
Which yields the result
```
+----------+-------+
|       tag|      c|
+----------+-------+
|javascript|1643617|
|    python|1352904|
|      java|1218220|
|   android| 913638|
|       php| 911806|
|        c#| 905331|
|      html| 769499|
|    jquery| 608071|
|       css| 510343|
|       c++| 458938|
+----------+-------+
```
A second option is to use the `query` option like this:
```
df = spark.read.format("bigquery").option("query", sql).load()
```

Notice that the execution should be faster as only the result is transmitted
over the wire. In a similar fashion the queries can include JOINs more
efficiently then running joins on Spark or use other BigQuery features such as
[subqueries](https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries),
[BigQuery user defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions),
[wildcard tables](https://cloud.google.com/bigquery/docs/reference/standard-sql/wildcard-table-reference),
[BigQuery ML](https://cloud.google.com/bigquery-ml/docs)
and more.

In order to use this feature the following configurations MUST be set:
* `viewsEnabled` must be set to `true`.
* `materializationDataset` must be set to a dataset where the GCP user has table
  creation permission. `materializationProject` is optional.

**Note:** As mentioned in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/writing-results#temporary_and_permanent_tables),
the queried tables must be in the same location as the `materializationDataset`.
Also, if the tables in the `SQL statement` are from projects other than the
`parentProject` then use the fully qualified table name i.e.
`[project].[dataset].[table]`.

**Important:** This feature is implemented by running the query on BigQuery and
saving the result into a temporary table, of which Spark will read the results
from. This may add additional costs on your BigQuery account.

### Reading From Views

The connector has a preliminary support for reading from
[BigQuery views](https://cloud.google.com/bigquery/docs/views-intro). Please
note there are a few caveats:

* BigQuery views are not materialized by default, which means that the connector
  needs to materialize them before it can read them. This process affects the
  read performance, even before running any `collect()` or `count()` action.
* The materialization process can also incur additional costs to your BigQuery
  bill.
* By default, the materialized views are created in the same project and
  dataset. Those can be configured by the optional `materializationProject`
  and `materializationDataset` options, respectively. These options can also
  be globally set by calling `spark.conf.set(...)` before reading the views.
* Reading from views is **disabled** by default. In order to enable it,
  either set the viewsEnabled option when reading the specific view
  (`.option("viewsEnabled", "true")`) or set it globally by calling
  `spark.conf.set("viewsEnabled", "true")`.
* As mentioned in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/writing-results#temporary_and_permanent_tables),
  the `materializationDataset` should be in same location as the view.

### Writing data to BigQuery

Writing DataFrames to BigQuery can be done using two methods: Direct and Indirect.

#### Direct write using the BigQuery Storage Write API

In this method the data is written directly to BigQuery using the
[BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api). In order to enable this option, please
set the `writeMethod` option to `direct`, as shown below:

```
df.write \
  .format("bigquery") \
  .option("writeMethod", "direct") \
  .save("dataset.table")
```

Writing to existing partitioned tables (date partitioned, ingestion time partitioned and range
partitioned) in APPEND save mode is fully supported by the connector and the BigQuery Storage Write
API. Partition overwrite and the use of `datePartition`, `partitionField`, `partitionType`, `partitionRangeStart`, `partitionRangeEnd`, `partitionRangeInterval` as
described below is not supported at this moment by the direct write method.

**Important:** Please refer to the [data ingestion pricing](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing)
page regarding the BigQuery Storage Write API pricing.

**Important:** Please use version 0.24.2 and above for direct writes, as previous
versions have a bug that may cause a table deletion in certain cases.

#### Indirect write
In this method the data is written first  to GCS, and then it is loaded it to BigQuery. A GCS bucket must be configured
to indicate the temporary data location.

```
df.write \
  .format("bigquery") \
  .option("temporaryGcsBucket","some-bucket") \
  .save("dataset.table")
```

The data is temporarily stored using the [Apache Parquet](https://parquet.apache.org/),
[Apache ORC](https://orc.apache.org/) or [Apache Avro](https://avro.apache.org/) formats.

The GCS bucket and the format can also be set globally using Spark's RuntimeConfig like this:
```
spark.conf.set("temporaryGcsBucket","some-bucket")
df.write \
  .format("bigquery") \
  .save("dataset.table")
```

When streaming a DataFrame to BigQuery, each batch is written in the same manner as a non-streaming DataFrame.
Note that a HDFS compatible
[checkpoint location](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
(eg: `path/to/HDFS/dir` or `gs://checkpoint-bucket/checkpointDir`) must be specified.

```
df.writeStream \
  .format("bigquery") \
  .option("temporaryGcsBucket","some-bucket") \
  .option("checkpointLocation", "some-location") \
  .option("table", "dataset.table")
```

**Important:** The connector does not configure the GCS connector, in order to avoid conflict with another GCS connector, if exists. In order to use the write capabilities of the connector, please configure the GCS connector on your cluster as explained [here](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/gcs).

### Properties

The API Supports a number of options to configure the read

<!--- TODO(#2): Convert to markdown -->
<table id="propertytable">
<style>
table#propertytable td, table th
{
word-break:break-word
}
</style>
  <tr valign="top">
   <th style="min-width:240px">Property</th>
   <th>Meaning</th>
   <th style="min-width:80px">Usage</th>
  </tr>
  <tr valign="top">
   <td><code>table</code>
   </td>
   <td>The BigQuery table in the format <code>[[project:]dataset.]table</code>.
       It is recommended to use the <code>path</code> parameter of
       <code>load()</code>/<code>save()</code> instead. This option has been
       deprecated and will be removed in a future version.
       <br/><strong>(Deprecated)</strong>
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>dataset</code>
   </td>
   <td>The dataset containing the table. This option should be used with
   standard table and views, but not when loading query results.
       <br/>(Optional unless omitted in <code>table</code>)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>project</code>
   </td>
   <td>The Google Cloud Project ID of the table. This option should be used with
   standard table and views, but not when loading query results.
       <br/>(Optional. Defaults to the project of the Service Account being used)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>parentProject</code>
   </td>
   <td>The Google Cloud Project ID of the table to bill for the export.
       <br/>(Optional. Defaults to the project of the Service Account being used)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>maxParallelism</code>
   </td>
   <td>The maximal number of partitions to split the data into. Actual number
       may be less if BigQuery deems the data small enough. If there are not
       enough executors to schedule a reader per partition, some partitions may
       be empty.
       <br/><b>Important:</b> The old parameter (<code>parallelism</code>) is
            still supported but in deprecated mode. It will ve removed in
            version 1.0 of the connector.
       <br/>(Optional. Defaults to the larger of the preferredMinParallelism and 20,000)</a>.)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>preferredMinParallelism</code>
   </td>
   <td>The preferred minimal number of partitions to split the data into. Actual number
       may be less if BigQuery deems the data small enough. If there are not
       enough executors to schedule a reader per partition, some partitions may
       be empty.
       <br/>(Optional. Defaults to the smallest of 3 times the application's default parallelism
       and maxParallelism</a>.)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>viewsEnabled</code>
   </td>
   <td>Enables the connector to read from views and not only tables. Please read
       the <a href="#reading-from-views">relevant section</a> before activating
       this option.
       <br/>(Optional. Defaults to <code>false</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>materializationProject</code>
   </td>
   <td>The project id where the materialized view is going to be created
       <br/>(Optional. Defaults to view's project id)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>materializationDataset</code>
   </td>
   <td>The dataset where the materialized view is going to be created. This
   dataset should be in same location as the view or the queried tables.
       <br/>(Optional. Defaults to view's dataset)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>materializationExpirationTimeInMinutes</code>
   </td>
   <td>The expiration time of the temporary table holding the materialized data
       of a view or a query, in minutes. Notice that the connector may re-use
       the temporary table due to the use of local cache and in order to reduce
       BigQuery computation, so very low values may cause errors. The value must
       be a positive integer.
       <br/>(Optional. Defaults to 1440, or 24 hours)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>readDataFormat</code>
   </td>
   <td>Data Format for reading from BigQuery. Options : <code>ARROW</code>, <code>AVRO</code>
	Unsupported Arrow filters are not pushed down and results are filtered later by Spark.
	(Currently Arrow does not suport disjunction across columns).
       <br/>(Optional. Defaults to <code>ARROW</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>optimizedEmptyProjection</code>
   </td>
   <td>The connector uses an optimized empty projection (select without any
       columns) logic, used for <code>count()</code> execution. This logic takes
       the data directly from the table metadata or performs a much efficient
       `SELECT COUNT(*) WHERE...` in case there is a filter. You can cancel the
       use of this logic by setting this option to <code>false</code>.
       <br/>(Optional, defaults to <code>true</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>pushAllFilters</code>
   </td>
   <td>If set to <code>true</code>, the connector pushes all the filters Spark can delegate
       to BigQuery Storage API. This reduces amount of data that needs to be sent from
       BigQuery Storage API servers to Spark clients.
       <br/>(Optional, defaults to <code>true</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>bigQueryJobLabel</code>
   </td>
   <td>Can be used to add labels to the connector initiated query and load
       BigQuery jobs. Multiple labels can be set.
       <br/>(Optional)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>bigQueryTableLabel</code>
   </td>
   <td>Can be used to add labels to the table while writing to a table. Multiple
       labels can be set.
       <br/>(Optional)
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>traceApplicationName</code>
   </td>
   <td>Application name used to trace BigQuery Storage read and write sessions.
       Setting the application name is required to set the trace ID on the
       sessions.
       <br/>(Optional)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>traceJobId</code>
   </td>
   <td>Job ID used to trace BigQuery Storage read and write sessions.
       <br/>(Optional, defaults to the Dataproc job ID is exists, otherwise uses
       the Spark application ID)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
     <td><code>createDisposition</code>
      </td>
      <td>Specifies whether the job is allowed to create new tables. The permitted
          values are:
          <ul>
            <li><code>CREATE_IF_NEEDED</code> - Configures the job to create the
                table if it does not exist.</li>
            <li><code>CREATE_NEVER</code> - Configures the job to fail if the
                table does not exist.</li>
          </ul>
          This option takes place only in case Spark has decided to write data
          to the table based on the SaveMode.
         <br/>(Optional. Default to CREATE_IF_NEEDED).
      </td>
      <td>Write</td>
   </tr>

  <tr valign="top">
   <td><code>writeMethod</code>
     </td>
       <td>Controls the method
       in which the data is written to BigQuery. Available values are <code>direct</code>
       to use the BigQuery Storage Write API and <code>indirect</code> which writes the
       data first to GCS and then triggers a BigQuery load operation. See more
       <a href="#writing-data-to-bigquery">here</a>
       <br/>(Optional, defaults to <code>indirect</code>)
     </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>writeAtLeastOnce</code>
   </td>
   <td>Guarantees that data is written to BigQuery at least once. This is a lesser
    guarantee than exactly once. This is suitable for streaming scenarios
    in which data is continuously being written in small batches.
       <br/>(Optional. Defaults to <code>false</code>)
       <br/><i>Supported only by the `DIRECT` write method and mode is <b>NOT</b> `Overwrite`.</i>
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>temporaryGcsBucket</code>
   </td>
   <td>The GCS bucket that temporarily holds the data before it is loaded to
       BigQuery. Required unless set in the Spark configuration
       (<code>spark.conf.set(...)</code>).
       <br/><i>Not supported by the `DIRECT` write method.</i>
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>persistentGcsBucket</code>
   </td>
   <td>The GCS bucket that holds the data before it is loaded to
       BigQuery. If informed, the data won't be deleted after write data
       into BigQuery.
       <br/><i>Not supported by the `DIRECT` write method.</i>
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>persistentGcsPath</code>
   </td>
   <td>The GCS path that holds the data before it is loaded to
       BigQuery. Used only with <code>persistentGcsBucket</code>.
       <br/><i>Not supported by the `DIRECT` write method.</i>
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>intermediateFormat</code>
   </td>
   <td>The format of the data before it is loaded to BigQuery, values can be
       either "parquet","orc" or "avro". In order to use the Avro format, the
       spark-avro package must be added in runtime.
       <br/>(Optional. Defaults to <code>parquet</code>). On write only.
       <br/><i>Supported only by the `spark-bigquery-with-dependencies_2.XX` connectors and just for the `INDIRECT` write method. The `spark-X.Y-bigquery` connectors use only AVRO as an intermediate format.</i>
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>useAvroLogicalTypes</code>
   </td>
   <td>When loading from Avro (`.option("intermediateFormat", "avro")`), BigQuery uses the underlying Avro types instead of the logical types [by default](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#logical_types). Supplying this option converts Avro logical types to their corresponding BigQuery data types.
       <br/>(Optional. Defaults to <code>false</code>). On write only.
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>datePartition</code>
   </td>
   <td>The date partition the data is going to be written to. Should be a date string
       given in the format <code>YYYYMMDD</code>. Can be used to overwrite the data of
	   a single partition, like this: <code><br/>df.write.format("bigquery")
       <br/>&nbsp;&nbsp;.option("datePartition", "20220331")
       <br/>&nbsp;&nbsp;.mode("overwrite")
       <br/>&nbsp;&nbsp;.save("table")</code>
       <br/>(Optional). On write only.
        <br/> Can also be used with different partition types like:
        <br/> HOUR: <code>YYYYMMDDHH</code>
        <br/> MONTH: <code>YYYYMM</code>
        <br/> YEAR: <code>YYYY</code>
        <br/><i>Not supported by the `DIRECT` write method.</i>
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
     <td><code>partitionField</code>
     </td>
     <td>If this field is specified, the table is partitioned by this field.
         <br/>For Time partitioning, specify together with the option `partitionType`.
         <br/>For Integer-range partitioning, specify together with the 3 options: `partitionRangeStart`, `partitionRangeEnd, `partitionRangeInterval`.
         <br/>The field must be a top-level TIMESTAMP or DATE field for Time partitioning, or INT64 for Integer-range partitioning. Its mode must be <strong>NULLABLE</strong>
         or <strong>REQUIRED</strong>.
         If the option is not set for a Time partitioned table, then the table will be partitioned by pseudo
         column, referenced via either<code>'_PARTITIONTIME' as TIMESTAMP</code> type, or
         <code>'_PARTITIONDATE' as DATE</code> type.
         <br/>(Optional).
         <br/><i>Not supported by the `DIRECT` write method.</i>
     </td>
     <td>Write</td>
    </tr>
   <tr valign="top">
    <td><code>partitionExpirationMs</code>
     </td>
     <td>Number of milliseconds for which to keep the storage for partitions in the table.
         The storage in a partition will have an expiration time of its partition time plus this value.
        <br/>(Optional).
        <br/><i>Not supported by the `DIRECT` write method.</i>
     </td>
     <td>Write</td>
   </tr>
   <tr valign="top">
       <td><code>partitionType</code>
        </td>
        <td>Used to specify Time partitioning.
            <br/>Supported types are: <code>HOUR, DAY, MONTH, YEAR</code>
            <br/> This option is <b>mandatory</b> for a target table to be Time partitioned.
            <br/>(Optional. Defaults to DAY if PartitionField is specified).
            <br/><i>Not supported by the `DIRECT` write method.</i>
       </td>
        <td>Write</td>
     </tr>
   <tr valign="top">
       <td><code>partitionRangeStart</code>,
           <code>partitionRangeEnd</code>,
           <code>partitionRangeInterval</code>
        </td>
        <td>Used to specify Integer-range partitioning.
            <br/>These options are <b>mandatory</b> for a target table to be Integer-range partitioned.
            <br/>All 3 options must be specified.
            <br/><i>Not supported by the `DIRECT` write method.</i>
       </td>
        <td>Write</td>
   </tr>
    <tr valign="top">
           <td><code>clusteredFields</code>
            </td>
            <td>A string of non-repeated, top level columns seperated by comma.
               <br/>(Optional).
            </td>
            <td>Write</td>
         </tr>
   <tr valign="top">
       <td><code>allowFieldAddition</code>
        </td>
        <td>Adds the <a href="https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/JobInfo.SchemaUpdateOption.html#ALLOW_FIELD_ADDITION" target="_blank">ALLOW_FIELD_ADDITION</a>
            SchemaUpdateOption to the BigQuery LoadJob. Allowed values are <code>true</code> and <code>false</code>.
           <br/>(Optional. Default to <code>false</code>).
           <br/><i>Supported only by the `INDIRECT` write method.</i>
        </td>
        <td>Write</td>
     </tr>
   <tr valign="top">
       <td><code>allowFieldRelaxation</code>
        </td>
        <td>Adds the <a href="https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/JobInfo.SchemaUpdateOption.html#ALLOW_FIELD_RELAXATION" target="_blank">ALLOW_FIELD_RELAXATION</a>
            SchemaUpdateOption to the BigQuery LoadJob. Allowed values are <code>true</code> and <code>false</code>.
           <br/>(Optional. Default to <code>false</code>).
           <br/><i>Supported only by the `INDIRECT` write method.</i>
        </td>
        <td>Write</td>
     </tr>
   <tr valign="top">
     <td><code>proxyAddress</code>
     </td>
     <td> Address of the proxy server. The proxy must be a HTTP proxy and address should be in the `host:port` format.
          Can be alternatively set in the Spark configuration (<code>spark.conf.set(...)</code>) or in Hadoop
          Configuration (<code>fs.gs.proxy.address</code>).
          <br/> (Optional. Required only if connecting to GCP via proxy.)
     </td>
     <td>Read/Write</td>
   </tr>
   <tr valign="top">
     <td><code>proxyUsername</code>
     </td>
     <td> The userName used to connect to the proxy. Can be alternatively set in the Spark configuration
          (<code>spark.conf.set(...)</code>) or in Hadoop Configuration (<code>fs.gs.proxy.username</code>).
          <br/> (Optional. Required only if connecting to GCP via proxy with authentication.)
     </td>
     <td>Read/Write</td>
   </tr>
   <tr valign="top">
     <td><code>proxyPassword</code>
     </td>
     <td> The password used to connect to the proxy. Can be alternatively set in the Spark configuration
          (<code>spark.conf.set(...)</code>) or in Hadoop Configuration (<code>fs.gs.proxy.password</code>).
          <br/> (Optional. Required only if connecting to GCP via proxy with authentication.)
     </td>
     <td>Read/Write</td>
   </tr>
   <tr valign="top">
     <td><code>httpMaxRetry</code>
     </td>
     <td> The maximum number of retries for the low-level HTTP requests to BigQuery. Can be alternatively set in the
          Spark configuration (<code>spark.conf.set("httpMaxRetry", ...)</code>) or in Hadoop Configuration
          (<code>fs.gs.http.max.retry</code>).
          <br/> (Optional. Default is 10)
     </td>
     <td>Read/Write</td>
   </tr>
   <tr valign="top">
     <td><code>httpConnectTimeout</code>
     </td>
     <td> The timeout in milliseconds to establish a connection with BigQuery. Can be alternatively set in the
          Spark configuration (<code>spark.conf.set("httpConnectTimeout", ...)</code>) or in Hadoop Configuration
          (<code>fs.gs.http.connect-timeout</code>).
          <br/> (Optional. Default is 60000 ms. 0 for an infinite timeout, a negative number for 20000)
     </td>
     <td>Read/Write</td>
   </tr>
   <tr valign="top">
     <td><code>httpReadTimeout</code>
     </td>
     <td> The timeout in milliseconds to read data from an established connection. Can be alternatively set in the
          Spark configuration (<code>spark.conf.set("httpReadTimeout", ...)</code>) or in Hadoop Configuration
          (<code>fs.gs.http.read-timeout</code>).
          <br/> (Optional. Default is 60000 ms. 0 for an infinite timeout, a negative number for 20000)
     </td>
     <td>Read</td>
   </tr>
   <tr valign="top">
     <td><code>arrowCompressionCodec</code>
     </td>
     <td>  Compression codec while reading from a BigQuery table when using Arrow format. Options :
           <code>ZSTD (Zstandard compression)</code>,
           <code>LZ4_FRAME (https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md)</code>,
           <code>COMPRESSION_UNSPECIFIED</code>. The recommended compression codec is <code>ZSTD</code>
           while using Java.
          <br/> (Optional. Defaults to <code>COMPRESSION_UNSPECIFIED</code> which means no compression will be used)
     </td>
     <td>Read</td>
   </tr>
   <tr valign="top">
     <td><code>cacheExpirationTimeInMinutes</code>
     </td>
     <td>  The expiration time of the in-memory cache storing query information.
          <br/> To disable caching, set the value to 0.
          <br/> (Optional. Defaults to 15 minutes)
     </td>
     <td>Read</td>
   </tr>
   <tr valign="top">
     <td><code>enableModeCheckForSchemaFields</code>
     </td>
     <td>  Checks the mode of every field in destination schema to be equal to the mode in corresponding source field schema, during DIRECT write.
          <br/> Default value is true i.e., the check is done by default. If set to false the mode check is ignored.
     </td>
     <td>Write</td>
  </tr>
     <td><code>enableListInference</code>
     </td>
     <td>  Indicates whether to use schema inference specifically when the mode is Parquet (https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#parquetoptions).
        <br/> Defaults to false.
        <br/>
     </td>
     <td>Write</td>
   </tr>
  <tr>
    <td><code>bqChannelPoolSize</code></td>
    <td>  The (fixed) size of the gRPC channel pool created by the BigQueryReadClient.
        <br/>For optimal performance, this should be set to at least the number of cores on the cluster executors.
    </td>
    <td>Read</td>
  </tr>
   <tr>
     <td><code>createReadSessionTimeoutInSeconds</code>
     </td>
     <td> The timeout in seconds to create a ReadSession when reading a table.
          <br/> For Extremely large table this value should be increased.
          <br/> (Optional. Defaults to 600 seconds)
     </td>
     <td>Read</td>
   </tr>
   <tr>
     <td><code>datetimeZoneId</code>
     </td>
     <td> The time zone ID used to convert BigQuery's
          <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type">DATETIME</a>
          into Spark's Timestamp, and vice versa.
          <br/> The value should be a legal <a href="https://en.wikipedia.org/wiki/List_of_tz_database_time_zones">time zone name</a>,
          that appears is accepted by Java's <code>java.time.ZoneId</code>. The full list can be
          seen by running <code>java.time.ZoneId.getAvailableZoneIds()</code> in Java/Scala, or
          <code>sc._jvm.java.time.ZoneId.getAvailableZoneIds()</code> in pyspark.
          <br/> (Optional. Defaults to <code>UTC</code>)
     </td>
     <td>Read/Write</td>
   </tr>
  <tr>
     <td><code>queryJobPriority</code>
     </td>
     <td> Priority levels set for the job while reading data from BigQuery query. The permitted values are:
          <ul>
            <li><code>BATCH</code> - Query is queued and started as soon as idle resources are available, usually within a few minutes. If the query hasn't started within 3 hours, its priority is changed to <code>INTERACTIVE</code>.</li>
            <li><code>INTERACTIVE</code> - Query is executed as soon as possible and count towards the concurrent rate limit and the daily rate limit.</li>
          </ul>
          For WRITE, this option will be effective when DIRECT write is used with OVERWRITE mode, where the connector overwrites the destination table using MERGE statement.
          <br/> (Optional. Defaults to <code>INTERACTIVE</code>)
     </td>
     <td>Read/Write</td>
   </tr>
  <tr>
     <td><code>destinationTableKmsKeyName</code>
     </td>
     <td>Describes the Cloud KMS encryption key that will be used to protect destination BigQuery
         table. The BigQuery Service Account associated with your project requires access to this
         encryption key. for further Information about using CMEK with BigQuery see
         [here](https://cloud.google.com/bigquery/docs/customer-managed-encryption#key_resource_id).
         <br/><b>Notice:</b> The table will be encrypted by the key only if it created by the
         connector. A pre-existing unencrypted table won't be encrypted just by setting this option.
         <br/> (Optional)
     </td>
     <td>Write</td>
   </tr>
</table>

Options can also be set outside of the code, using the `--conf` parameter of `spark-submit` or `--properties` parameter
of the `gcloud dataproc submit spark`. In order to use this, prepend the prefix `spark.datasource.bigquery.` to any of
the options, for example `spark.conf.set("temporaryGcsBucket", "some-bucket")` can also be set as
`--conf spark.datasource.bigquery.temporaryGcsBucket=some-bucket`.

### Data types

With the exception of `DATETIME` and `TIME` all BigQuery data types directed map into the corresponding Spark SQL data type. Here are all of the mappings:

<!--- TODO(#2): Convert to markdown -->
<table>
  <tr valign="top">
   <td><strong>BigQuery Standard SQL Data Type </strong>
   </td>
   <td><strong>Spark SQL</strong>
<p>
<strong>Data Type</strong>
   </td>
   <td><strong>Notes</strong>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>BOOL</code></strong>
   </td>
   <td><strong><code>BooleanType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>INT64</code></strong>
   </td>
   <td><strong><code>LongType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>FLOAT64</code></strong>
   </td>
   <td><strong><code>DoubleType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>NUMERIC</code></strong>
   </td>
   <td><strong><code>DecimalType</code></strong>
   </td>
   <td>
     Please refer to <a href="#numeric-and-bignumeric-support">Numeric and BigNumeric support</a>
   </td>
  </tr>
  <tr valign="top">
     <td><strong><code>BIGNUMERIC</code></strong>
     </td>
     <td><strong><code>DecimalType</code></strong>
     </td>
     <td>
       Please refer to <a href="#numeric-and-bignumeric-support">Numeric and BigNumeric support</a>
     </td>
    </tr>
  <tr valign="top">
   <td><strong><code>STRING</code></strong>
   </td>
   <td><strong><code>StringType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>BYTES</code></strong>
   </td>
   <td><strong><code>BinaryType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>STRUCT</code></strong>
   </td>
   <td><strong><code>StructType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>ARRAY</code></strong>
   </td>
   <td><strong><code>ArrayType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>TIMESTAMP</code></strong>
   </td>
   <td><strong><code>TimestampType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>DATE</code></strong>
   </td>
   <td><strong><code>DateType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>DATETIME</code></strong>
   </td>
   <td><strong><code>TimestampType</code></strong>
   </td>
   <td>Spark has no DATETIME type. The value is casted as a local time in the `datetimeZoneId` time zone.
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>TIME</code></strong>
   </td>
   <td><strong><code>LongType</code>, <strong><code>StringType</code>*</strong>
   </td>
   <td>Spark has no TIME type. The generated longs, which indicate <a href="https://avro.apache.org/docs/1.8.0/spec.html#Time+%2528microsecond+precision%2529">microseconds since midnight</a> can be safely cast to TimestampType, but this causes the date to be inferred as the current day. Thus times are left as longs and user can cast if they like.
<p>
When casting to Timestamp TIME have the same TimeZone issues as DATETIME
<p>
* Spark string can be written to an existing BQ TIME column provided it is in the <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#canonical_format_for_time_literals">format for BQ TIME literals</a>.
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>JSON</code></strong>
   </td>
   <td><strong><code>StringType</code></strong>
   </td>
   <td>Spark has no JSON type. The values are read as String. In order to write JSON back to BigQuery, the following conditions are <b>REQUIRED</b>:
       <ul>
          <li>Use the <code>INDIRECT</code> write method</li>
          <li>Use the <code>AVRO</code> intermediate format</li>
          <li>The DataFrame field <b>MUST</b> be of type <code>String</code> and has an entry of sqlType=JSON in its metadata</li>
       </ul>
   </td>
  </tr>
  <tr valign="top" id="datatype:map">
   <td><strong><code>ARRAY&lt;STRUCT&lt;key,value&gt;&gt;</code></strong>
   </td>
   <td><strong><code>MapType</code></strong>
   </td>
   <td>BigQuery has no MAP type, therefore similar to other conversions like Apache Avro and BigQuery Load jobs, the connector converts a Spark Map to a REPEATED STRUCT&lt;key,value&gt;.
       This means that while writing and reading of maps is available, running a SQL on BigQuery that uses map semantics is not supported.
       To refer to the map's values using BigQuery SQL, please check the <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays">BigQuery documentation</a>.
       Due to these incompatibilities, a few restrictions apply:
       <ul>
          <li>Keys can be Strings only</li>
          <li>Values can be simple types (not structs)</li>
          <li>For INDIRECT write, use the <code>AVRO</code> intermediate format. DIRECT write is supported as well</li>
       </ul>
   </td>
  </tr>
</table>

#### Spark ML Data Types Support

The Spark ML [Vector](https://spark.apache.org/docs/2.4.5/api/python/pyspark.ml.html#pyspark.ml.linalg.Vector) and
[Matrix](https://spark.apache.org/docs/2.4.5/api/python/pyspark.ml.html#pyspark.ml.linalg.Matrix) are supported,
including their dense and sparse versions. The data is saved as a BigQuery RECORD. Notice that a suffix is added to
the field's description which includes the spark type of the field.

In order to write those types to BigQuery, use the ORC or Avro intermediate format, and have them as column of the
Row (i.e. not a field in a struct).

#### Numeric and BigNumeric support
BigQuery's BigNumeric has a precision of 76.76 (the 77th digit is partial) and scale of 38. Since
this precision and scale is beyond spark's DecimalType (38 scale and 38 precision) support, it means
that BigNumeric fields with precision larger than 38 cannot be used. Once this Spark limitation will
be updated the connector will be updated accordingly.

The Spark Decimal/BigQuery Numeric conversion tries to preserve the parameterization of the type, i.e
`NUMERIC(10,2)` will be converted to `Decimal(10,2)` and vice versa. Notice however that there are
cases where [the parameters are lost](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#parameterized_data_types).
This means that the parameters will be reverted to the defaults - NUMERIC (38,9) and BIGNUMERIC(76,38).
This means that at the moment, BigNumeric read is supported only from a standard table, but not from
BigQuery view or when [reading data from a BigQuery query](#reading-data-from-a-bigquery-query).

### Filtering

The connector automatically computes column and pushdown filters the DataFrame's `SELECT` statement e.g.

```
spark.read.bigquery("bigquery-public-data:samples.shakespeare")
  .select("word")
  .where("word = 'Hamlet' or word = 'Claudius'")
  .collect()
```


filters to the column `word`  and pushed down the predicate filter `word = 'hamlet' or word = 'Claudius'`.

If you do not wish to make multiple read requests to BigQuery, you can cache the DataFrame before filtering e.g.:

```
val cachedDF = spark.read.bigquery("bigquery-public-data:samples.shakespeare").cache()
val rows = cachedDF.select("word")
  .where("word = 'Hamlet'")
  .collect()
// All of the table was cached and this doesn't require an API call
val otherRows = cachedDF.select("word_count")
  .where("word = 'Romeo'")
  .collect()
```

You can also manually specify the `filter` option, which will override automatic pushdown and Spark will do the rest of the filtering in the client.

### Partitioned Tables

The pseudo columns \_PARTITIONDATE and \_PARTITIONTIME are not part of the table schema. Therefore in order to query by the partitions of [partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) do not use the where() method shown above. Instead, add a filter option in the following manner:

```
val df = spark.read.format("bigquery")
  .option("filter", "_PARTITIONDATE > '2019-01-01'")
  ...
  .load(TABLE)
```

### Configuring Partitioning

By default the connector creates one partition per 400MB in the table being read (before filtering). This should roughly correspond to the maximum number of readers supported by the BigQuery Storage API.
This can be configured explicitly with the <code>[maxParallelism](#properties)</code> property. BigQuery may limit the number of partitions based on server constraints.

## Tagging BigQuery Resources

In order to support tracking the usage of BigQuery resources the connectors
offers the following options to tag BigQuery resources:

### Adding BigQuery Jobs Labels

The connector can launch BigQuery load and query jobs. Adding labels to the jobs
is done in the following manner:
```
spark.conf.set("bigQueryJobLabel.cost_center", "analytics")
spark.conf.set("bigQueryJobLabel.usage", "nightly_etl")
```
This will create labels `cost_center`=`analytics` and `usage`=`nightly_etl`.

### Adding BigQuery Storage Trace ID

Used to annotate the read and write sessions. The trace ID is of the format
`Spark:ApplicationName:JobID`. This is an opt-in option, and to use it the user
need to set the `traceApplicationName` property. JobID is auto generated by the
Dataproc job ID, with a fallback to the Spark application ID (such as
`application_1648082975639_0001`). The Job ID can be overridden by setting the
`traceJobId` option. Notice that the total length of the trace ID cannot be over
256 characters.

## Using in Jupyter Notebooks

The connector can be used in [Jupyter notebooks](https://jupyter.org/) even if
it is not installed on the Spark cluster. It can be added as an external jar in
using the following code:

**Python:**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder
  .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:${next-release-tag}")
  .getOrCreate()
df = spark.read.format("bigquery")
  .load("dataset.table")
```

**Scala:**
```python
val spark = SparkSession.builder
.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:${next-release-tag}")
.getOrCreate()
val df = spark.read.format("bigquery")
.load("dataset.table")
```

In case Spark cluster is using Scala 2.12 (it's optional for Spark 2.4.x,
mandatory in 3.0.x), then the relevant package is
com.google.cloud.spark:spark-bigquery-with-dependencies_**2.12**:${next-release-tag}. In
order to know which Scala version is used, please run the following code:

**Python:**
```python
spark.sparkContext._jvm.scala.util.Properties.versionString()
```

**Scala:**
```python
scala.util.Properties.versionString
```
## Compiling against the connector

Unless you wish to use the implicit Scala API `spark.read.bigquery("TABLE_ID")`, there is no need to compile against the connector.

To include the connector in your project:

### Maven

```xml
<dependency>
  <groupId>com.google.cloud.spark</groupId>
  <artifactId>spark-bigquery-with-dependencies_${scala.version}</artifactId>
  <version>${next-release-tag}</version>
</dependency>
```

### SBT

```sbt
libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "${next-release-tag}"
```

### Connector metrics and how to view them

Spark populates a lot of metrics which can be found by the end user in the spark history page. But all these metrics are spark related which are implicitly collected without any change from the connector.
But there are few metrics which are populated from the BigQuery and currently are visible in the application logs which can be read in the driver/executor logs.

From Spark 3.2 onwards, spark has provided the API to expose custom metrics in the spark UI page https://spark.apache.org/docs/3.2.0/api/java/org/apache/spark/sql/connector/metric/CustomMetric.html

Currently, using this API, connector exposes the following bigquery metrics during read
<table id="metricstable">
<style>
table#metricstable td, table th
{
word-break:break-word
}
</style>
  <tr valign="top">
   <th style="min-width:240px">Metric Name</th>
   <th style="min-width:240px">Description</th>
  </tr>
  <tr valign="top">
   <td><code>bytes read</code></td>
   <td>number of BigQuery bytes read</td>
  </tr>
  <tr valign="top">
   <td><code>rows read</code></td>
   <td>number of BigQuery rows read</td>
  </tr>
  <tr valign="top">
   <td><code>scan time</code></td>
   <td>the amount of time spent between read rows response requested to obtained across all the executors, in milliseconds.</td>
  </tr>
  <tr valign="top">
   <td><code>parse time</code></td>
   <td>the amount of time spent for parsing the rows read across all the executors, in milliseconds.</td>
  </tr>
  <tr valign="top">
   <td><code>spark time</code></td>
   <td>the amount of time spent in spark to process the queries (i.e., apart from scanning and parsing), across all the executors, in milliseconds.</td>
  </tr>
</table>


**Note:** To use the metrics in the Spark UI page, you need to make sure the `spark-bigquery-metrics-${next-release-tag}.jar` is the class path before starting the history-server and the connector version is `spark-3.2` or above.

## FAQ

### What is the Pricing for the Storage API?

See the [BigQuery pricing documentation](https://cloud.google.com/bigquery/pricing#storage-api).

### I have very few partitions

You can manually set the number of partitions with the `maxParallelism` property. BigQuery may provide fewer partitions than you ask for. See [Configuring Partitioning](#configuring-partitioning).

You can also always repartition after reading in Spark.

### I get quota exceeded errors while writing

If there are too many partitions the CreateWriteStream or Throughput [quotas](https://cloud.google.com/bigquery/quotas#write-api-limits)
may be exceeded. This occurs because while the data within each partition is processed serially, independent
partitions may be processed in parallel on different nodes within the spark cluster. Generally, to ensure maximum
sustained throughput you should file a quota increase request. However, you can also manually reduce the number of
partitions being written by calling <code>coalesce</code> on the DataFrame to mitigate this problem.

```
desiredPartitionCount = 5
dfNew = df.coalesce(desiredPartitionCount)
dfNew.write
```

A rule of thumb is to have a single partition handle at least 1GB of data.

Also note that a job running with the `writeAtLeastOnce` property turned on will not encounter CreateWriteStream
quota errors.

### How do I authenticate outside GCE / Dataproc?

The connector needs an instance of a GoogleCredentials in order to connect to the BigQuery APIs. There are multiple
options to provide it:

* The default is to load the JSON key from the  `GOOGLE_APPLICATION_CREDENTIALS` environment variable, as described
  [here](https://cloud.google.com/docs/authentication/getting-started).
* In case the environment variable cannot be changed, the credentials file can be configured as
  as a spark option. The file should reside on the same path on all the nodes of the cluster.
```
// Globally
spark.conf.set("credentialsFile", "</path/to/key/file>")
// Per read/Write
spark.read.format("bigquery").option("credentialsFile", "</path/to/key/file>")
```
* Credentials can also be provided explicitly, either as a parameter or from Spark runtime configuration.
  They should be passed in as a base64-encoded string directly.
```
// Globally
spark.conf.set("credentials", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
// Per read/Write
spark.read.format("bigquery").option("credentials", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
```
* In cases where the user has an internal service providing the Google AccessToken, a custom implementation
  can be done, creating only the AccessToken and providing its TTL. Token refresh will re-generate a new token. In order
  to use this, implement the
  [com.google.cloud.bigquery.connector.common.AccessTokenProvider](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/master/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/AccessTokenProvider.java)
  interface. The fully qualified class name of the implementation should be provided in the `gcpAccessTokenProvider`
  option. `AccessTokenProvider` must be implemented in Java or other JVM language such as Scala or Kotlin. It must
  either have a no-arg constructor or a constructor accepting a single `java.util.String` argument. This configuration
  parameter can be supplied using the `gcpAccessTokenProviderConfig` option. If this is not provided then the no-arg
  constructor wil be called. The jar containing the implementation should be on the cluster's classpath.
```
// Globally
spark.conf.set("gcpAccessTokenProvider", "com.example.ExampleAccessTokenProvider")
// Per read/Write
spark.read.format("bigquery").option("gcpAccessTokenProvider", "com.example.ExampleAccessTokenProvider")
```
* Service account impersonation can be configured for a specific username and a group name, or
  for all users by default using below properties:

  - `gcpImpersonationServiceAccountForUser_<USER_NAME>` (not set by default)

    The service account impersonation for a specific user.

  - `gcpImpersonationServiceAccountForGroup_<GROUP_NAME>` (not set by default)

    The service account impersonation for a specific group.

  - `gcpImpersonationServiceAccount` (not set by default)

    Default service account impersonation for all users.

  If any of the above properties are set then the service account specified will be impersonated by
  generating a short-lived credentials when accessing BigQuery.

  If more than one property is set then the service account associated with the username will take
  precedence over the service account associated with the group name for a matching user and group,
  which in turn will take precedence over default service account impersonation.

* For a simpler application, where access token refresh is not required, another alternative is to pass the access token
  as the `gcpAccessToken` configuration option. You can get the access token by running
  `gcloud auth application-default print-access-token`.
```
// Globally
spark.conf.set("gcpAccessToken", "<access-token>")
// Per read/Write
spark.read.format("bigquery").option("gcpAccessToken", "<acccess-token>")
```

**Important:** The `CredentialsProvider` and  `AccessTokenProvider` need to be implemented in Java or
other JVM language such as Scala or Kotlin. The jar containing the implementation should be on the cluster's classpath.

**Notice:** Only one of the above options should be provided.

### How do I connect to GCP/BigQuery via Proxy?

To connect to a forward proxy and to authenticate the user credentials, configure the following options.

`proxyAddress`: Address of the proxy server. The proxy must be an HTTP proxy and address should be in the `host:port`
format.

`proxyUsername`: The userName used to connect to the proxy.

`proxyPassword`: The password used to connect to the proxy.

```
val df = spark.read.format("bigquery")
  .option("proxyAddress", "http://my-proxy:1234")
  .option("proxyUsername", "my-username")
  .option("proxyPassword", "my-password")
  .load("some-table")
```

The same proxy parameters can also be set globally using Spark's RuntimeConfig like this:

```
spark.conf.set("proxyAddress", "http://my-proxy:1234")
spark.conf.set("proxyUsername", "my-username")
spark.conf.set("proxyPassword", "my-password")

val df = spark.read.format("bigquery")
  .load("some-table")
```

You can set the following in the hadoop configuration as well.

`fs.gs.proxy.address`(similar to "proxyAddress"), `fs.gs.proxy.username`(similar to "proxyUsername") and
`fs.gs.proxy.password`(similar to "proxyPassword").

If the same parameter is set at multiple places the order of priority is as follows:

option("key", "value") > spark.conf > hadoop configuration
