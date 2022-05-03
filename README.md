# Apache Spark SQL connector for Google BigQuery

<!--- TODO(#2): split out into more documents. -->

The connector supports reading [Google BigQuery](https://cloud.google.com/bigquery/) tables into Spark's DataFrames, and writing DataFrames back into BigQuery.
This is done by using the [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to communicate with BigQuery.

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

The API rebalances records between readers until they all complete. This means that all Map phases will finish nearly concurrently. See this blog article on [how dynamic sharding is similarly used in Google Cloud Dataflow](https://cloud.google.com/blog/big-data/2016/05/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow).

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

| version | Link |
| --- | --- |
| Scala 2.11 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.24.2.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.24.2.jar)) |
| Scala 2.12 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar)) |
| Spark 2.4  | `gs://spark-lib/bigquery/spark-2.4-bigquery-0.24.2-preview.jar`([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-2.4-bigquery-0.24.2-preview.jar)) |

The only difference between first two connectors is that the former is a Scala 2.11 based connector, targeting Spark 2.3
and 2.4 using Scala 2.11 whereas the latter is a Scala 2.12 based connector, targeting Spark 2.4 and 3.x using Scala 2.12.
There should not be any code differences between the 2 connectors.

Third version is a Java based connector targeting Spark 2.4 of all Scala versions built on the new Data Source APIs
(Data Source API v2) of Spark. It is still in preview mode.

**Note:** If you are using scala jars please use the jar relevant to your Spark installation. Starting from Spark 2.4 onwards there is an
option to use the Java only jar.

The connector is also available from the
[Maven Central](https://repo1.maven.org/maven2/com/google/cloud/spark/)
repository. It can be used using the `--packages` option or the
`spark.jars.packages` configuration property. Use the following value

| version | Connector Artifact |
| --- | --- |
| Scala 2.11 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.24.2` |
| Scala 2.12 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2` |
| Spark 2.4  | `com.google.cloud.spark:spark-2.4-bigquery:0.24.2-preview` |

## Hello World Example

You can run a simple PySpark wordcount against the API without compilation by running

**Dataproc image 1.5 and above**

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar \
  examples/python/shakespeare.py
```

**Dataproc image 1.4 and below**

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.24.2.jar \
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

#### Direct write using the BigQuery Storage Write API
The Spark 2.4 dedicated connector supports writing directly to BigQuery without first writing to GCS, using
the [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api)
to write data directly to BigQuery. In order to enable this option, please set the `writeMethod` option
to `direct`, as shown below:

```
df.write \
  .format("bigquery") \
  .option("writeMethod", "direct") \
  .save("dataset.table")
```

Partition overwriting and the use of `datePartition`, `partitionField` and `partitionType` as described below is not
supported at this moment by the direct write method.

**Important:** Please refer to the [data ingestion pricing](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing)
page regarding the BigQuery Storage Write API pricing.

**Important:** Please use version 0.24.2 and above for direct writes, as previous
versions have a bug that may cause a table deletion in certain cases.

#### Indirect write
This method is supported by all the connector. In this method the data is written first  to GCS and then
it is loaded it to BigQuery. A GCS bucket must be configured to indicate the temporary data location.

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
<table>
  <tr valign="top">
   <th>Property</th>
   <th>Meaning</th>
   <th>Usage</th>
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
       <br/>(Optional. Defaults to one partition per 400MB. See
       <a href="#configuring-partitioning">Configuring Partitioning</a>.)
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
       <td>Used only by the Spark 2.4 dedicated connector. Controls the method
       in which the data is written to BigQuery. Available values are <code>direct</code>
       to use the BigQuery Storage Write API and <code>indirect</code> which writes the
       data first to GCS and then triggers a BigQuery load operation. See more
       <a href="#writing-data-to-bigquery">here</a>
       <br/>(Optional, defaults to <code>indirect</code>)
     </td>
   <td>Write (supported only by the Spark 2.4 dedicated connector)</td>
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
       <br/><i>Not supported by the `DIRECT` write method.</i>
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
     <td>If field is specified together with `partitionType`, the table is partitioned by this field.
         The field must be a top-level TIMESTAMP or DATE field. Its mode must be <strong>NULLABLE</strong>
         or <strong>REQUIRED</strong>.
         If the option is not set for a partitioned table, then the table will be partitioned by pseudo
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
        <td>Supported types are: <code>HOUR, DAY, MONTH, YEAR</code>
            <br/> This option is <b>mandatory</b> for a target table to be partitioned.
            <br/>(Optional. Defaults to DAY if PartitionField is specified).
            <br/><i>Not supported by the `DIRECT` write method.</i>
       </td>
        <td>Write</td>
     </tr>
    <tr valign="top">
           <td><code>clusteredFields</code>
            </td>
            <td>Comma separated list of non-repeated, top level columns. Clustering is only supported for partitioned tables
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
        </td>
        <td>Write</td>
     </tr>
   <tr valign="top">
       <td><code>allowFieldRelaxation</code>
        </td>
        <td>Adds the <a href="https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/JobInfo.SchemaUpdateOption.html#ALLOW_FIELD_RELAXATION" target="_blank">ALLOW_FIELD_RELAXATION</a>
            SchemaUpdateOption to the BigQuery LoadJob. Allowed values are <code>true</code> and <code>false</code>.
           <br/>(Optional. Default to <code>false</code>).
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
     This preserves <code>NUMERIC</code>'s full 38 digits of precision and 9 digits of scope.
   </td>
  </tr>
  <tr valign="top">
     <td><strong><code>BIGNUMERIC</code></strong>
     </td>
     <td><strong><code>BigNumericUDT (UserDefinedType)</code></strong>
     </td>
     <td>
       Scala/Java: BigNumericUDT DataType internally uses java.math.BigDecimal to hold the BigNumeric data.
       <p> Python: BigNumericUDT DataType internally used python's Decimal class to hold the BigNumeric data.
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
   <td><strong><code>StringType</code></strong>
   </td>
   <td>Spark has no DATETIME type. Casting to TIMESTAMP uses a configured TimeZone, which defaults to the local timezone (UTC in GCE / Dataproc).
<p>
We are considering adding an optional TimeZone property to allow automatically  converting to TimeStamp, this would be consistent with Spark's handling of CSV/JSON (except they always try to convert when inferring schema, and default to the local timezone)
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>TIME</code></strong>
   </td>
   <td><strong><code>LongType</code></strong>
   </td>
   <td>Spark has no TIME type. The generated longs, which indicate <a href="https://avro.apache.org/docs/1.8.0/spec.html#Time+%2528microsecond+precision%2529">microseconds since midnight</a> can be safely cast to TimestampType, but this causes the date to be inferred as the current day. Thus times are left as longs and user can cast if they like.
<p>
When casting to Timestamp TIME have the same TimeZone issues as DATETIME
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

#### BigNumeric support
BigQuery's BigNumeric has a precision of 76.76 (the 77th digit is partial) and scale of 38. Since this precision and
scale is beyond spark's DecimalType (38 scale and 38 precision) support, the BigNumeric DataType is converted
into spark's [UserDefinedType](https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/types/UserDefinedType.html).
The BigNumeric data can be accessed via BigNumericUDT DataType which internally uses
[java.math.BigDecimal](https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html)
to hold the BigNumeric data. The data can be read in either AVRO or ARROW formats.

In order to write BigNumericUDT to BigQuery, use either ORC or PARQUET intermediate formats (currently we do not support
AVRO). Notice that the data gets written to BigQuery as String.

Code examples:

**Scala:**

```
import org.apache.spark.bigquery.BigNumeric

val df = spark.read
  .format("bigquery")
  .load("PROJECT.DATASET.TABLE")

val rows: Array[java.math.BigDecimal] = df
  .collect()
  .map(row => row.get("BIG_NUMERIC_COLUMN").asInstanceOf[BigNumeric].getNumber)

rows.foreach(value => System.out.println("BigNumeric value  " + value.toPlainString))
```

**Python:** Spark's [UserDefinedType](https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/types/UserDefinedType.html)
needs a separate implementation for Python. Corresponding python class(s) should be provided as config params while
creating the job or added during runtime. See examples below:

1) Adding python files while launching pyspark
```
# use appropriate version for jar depending on the scala version
pyspark --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.24.2.jar
  --py-files gs://spark-lib/bigquery/spark-bigquery-support-0.24.2.zip
  --files gs://spark-lib/bigquery/spark-bigquery-support-0.24.2.zip
```

2) Adding python files in Jupyter Notebook
```
from pyspark.sql import SparkSession
from pyspark import SparkFiles
# use appropriate version for jar depending on the scala version
spark = SparkSession.builder\
  .appName('BigNumeric')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.24.2.jar')\
  .config('spark.submit.pyFiles', 'gs://spark-lib/bigquery/spark-bigquery-support-0.24.2.zip')\
  .config('spark.files', 'gs://spark-lib/bigquery/spark-bigquery-support-0.24.2.zip')\
  .getOrCreate()

# extract the spark-bigquery-support zip file
import zipfile
with zipfile.ZipFile(SparkFiles.get("spark-bigquery-support-0.24.2.zip")) as zf:
  zf.extractall()
```

3) Adding Python files during runtime
```
# use appropriate version for jar depending on the scala version
spark = SparkSession.builder\
  .appName('BigNumeric')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.24.2.jar')\
  .getOrCreate()

spark.sparkContext.addPyFile("gs://spark-lib/bigquery/spark-bigquery-support-0.24.2.zip")
```

Usage Example:
```
df = spark.read.format("bigquery").load({project}.{dataset}.{table_name})
data = df.select({big_numeric_column_name}).collect()

for row in data:
  bigNumeric = row[{big_numeric_column_name}]
  # bigNumeric.number is instance of python's Decimal class
  print(str(bigNumeric.number))
```

In case the above code throws ModuleNotFoundError, please add the following code
before reading the BigNumeric data.

```
try:
    import pkg_resources

    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)
```

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
  .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.24.2")
  .getOrCreate()
df = spark.read.format("bigquery")
  .load("dataset.table")
```

**Scala:**
```python
val spark = SparkSession.builder
.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.24.2")
.getOrCreate()
val df = spark.read.format("bigquery")
.load("dataset.table")
```

In case Spark cluster is using Scala 2.12 (it's optional for Spark 2.4.x,
mandatory in 3.0.x), then the relevant package is
com.google.cloud.spark:spark-bigquery-with-dependencies_**2.12**:0.24.2. In
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
  <version>0.24.2</version>
</dependency>
```

### SBT

```sbt
libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.24.2"
```

## FAQ

### What is the Pricing for the Storage API?

See the [BigQuery pricing documentation](https://cloud.google.com/bigquery/pricing#storage-api).

### I have very few partitions

You can manually set the number of partitions with the `maxParallelism` property. BigQuery may provide fewer partitions than you ask for. See [Configuring Partitioning](#configuring-partitioning).

You can also always repartition after reading in Spark.

### How do I authenticate outside GCE / Dataproc?

Use a service account JSON key and `GOOGLE_APPLICATION_CREDENTIALS` as described [here](https://cloud.google.com/docs/authentication/getting-started).

Credentials can also be provided explicitly either as a parameter or from Spark runtime configuration.
It can be passed in as a base64-encoded string directly, or a file path that contains the credentials (but not both).

Example:
```
spark.read.format("bigquery").option("credentials", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
```
or
```
spark.conf.set("credentials", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
```

Alternatively, specify the credentials file name.

```
spark.read.format("bigquery").option("credentialsFile", "</path/to/key/file>")
```
or
```
spark.conf.set("credentialsFile", "</path/to/key/file>")
```

Another alternative to passing the credentials, is to pass the access token used for authenticating
the API calls to the Google Cloud Platform APIs. You can get the access token by running
`gcloud auth application-default print-access-token`.

```
spark.read.format("bigquery").option("gcpAccessToken", "<acccess-token>")
```
or
```
spark.conf.set("gcpAccessToken", "<access-token>")
```

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
