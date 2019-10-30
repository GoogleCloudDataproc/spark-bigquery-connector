# Apache Spark SQL connector for Google BigQuery (Beta)

<!--- TODO(#2): split out into more documents. -->

The connector supports reading [Google BigQuery](https://cloud.google.com/bigquery/) tables into Spark's DataFrames, and writing DataFrames back into BigQuery.
This is done by using the [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to communicate with BigQuery.

## Beta Disclaimer

The BigQuery Storage API and this connector are in Beta and are subject to change.

Changes may include, but are not limited to:
* Type conversion
* Partitioning
* Parameters

Breaking changes will be restricted to major and minor versions.

## BigQuery Storage API
The [Storage API](https://cloud.google.com/bigquery/docs/reference/storage) streams data in parallel directly from BigQuery via gRPC without using Google Cloud Storage as an intermediary.

It has a number of advantages over using the previous export-based read flow that should generally lead to better read performance:

### Direct Streaming

It does not leave any temporary files in Google Cloud Storage. Rows are read directly from BigQuery servers using an Avro wire format.

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

## Downloading the Connector

The latest version connector of the connector is publicly available in [gs://spark-lib/bigquery/spark-bigquery-latest.jar](https://console.cloud.google.com/storage/browser/spark-lib/bigquery). A Scala 2.12 compiled version exist in gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar.

## Hello World Example

You can run a simple PySpark wordcount against the API without compilation by running

<!--- TODO(pmkc): Update jar reference -->

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
  examples/python/shakespeare.py
```

### Example Codelab ###
https://codelabs.developers.google.com/codelabs/pyspark-bigquery

## Compiling against the connector

Unless you wish to use the implicit Scala API `spark.read.bigquery("TABLE_ID")`, there is no need to compile against the connector.

To include the connector in your project:

### Maven

```xml
<dependency>
  <groupId>com.google.cloud.spark</groupId>
  <artifactId>spark-bigquery_${scala.version}</artifactId>
  <version>0.9.1-beta</version>
  <classifier>shaded</classifier>
</dependency>
```

### SBT

```sbt
libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery" % "0.9.1-beta" classifier "shaded"
```

## API

The connector uses the cross language [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources):

### Reading data from BigQuery

```
df = spark.read
  .format("bigquery")
  .option("table", "publicdata.samples.shakespeare")
  .load()
```

or the Scala only implicit API:

```
import com.google.cloud.spark.bigquery._
val df = spark.read.bigquery("publicdata.samples.shakespeare")
```

See [Shakespeare.scala](src/main/scala/com/google/cloud/spark/bigquery/examples/Shakespeare.scala) and [shakespeare.py](examples/python/shakespeare.py) for more information.

### Writing data to BigQuery

Writing a DataFrame to BigQuery is done in a similar manner. Notice that the process writes the data first to GCS and then loads it to BigQuery, a GCS bucket must be configured to indicate the temporary data location.

```
df.write
  .format("bigquery")
  .option("table","dataset.table")
  .option("temporaryGcsBucket","some-bucket")
  .save()
```

The data is temporarily stored using the [Apache parquet](https://parquet.apache.org/) format. An alternative format is [Apache ORC](https://orc.apache.org/).

The GCS bucket and the format can also be set globally using Spark"s RuntimeConfig like this:
```
spark.conf.set("temporaryGcsBucket","some-bucket")
df.write
  .format("bigquery")
  .option("table","dataset.table")
  .save()
```


### Properties

The API Supports a number of options to configure the read

<!--- TODO(#2): Convert to markdown -->
<table>
  <tr>
   <th>Property</th>
   <th>Meaning</th>
   <th>Usage</th>
  </tr>
  <tr>
   <td><code>table</code>
   </td>
   <td>The BigQuery table in the format <code>[[project:]dataset.]table</code>. <strong>(Required)</strong>
   </td>
   <td>Read/Write</td>
  </tr>
  <tr>
   <td><code>dataset</code>
   </td>
   <td>The dataset containing the table.
       <br/>(Optional unless omitted in <code>table</code>)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr>
   <td><code>project</code>
   </td>
   <td>The Google Cloud Project ID of the table.
       <br/>(Optional. Defaults to the project of the Service Account being used)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr>
   <td><code>parentProject</code>
   </td>
   <td>The Google Cloud Project ID of the table to bill for the export.
       <br/>(Optional. Defaults to the project of the Service Account being used)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr>
   <td><code>parallelism</code>
   </td>
   <td>The number of partitions to split the data into. Actual number may be less
       if BigQuery deems the data small enough. If there are not enough executors
       to schedule a reader per partition, some partitions may be empty.
       <br/>(Optional. Defaults to one partition per 400MB. See
       <a href="#configuring-partitioning">Configuring Partitioning</a>.)
   </td>
   <td>Read</td>
  </tr>
  <tr>
   <td><code>temporaryGcsBucket</code>
   </td>
   <td>The GCS bucket that temporarily holds the data before it is loaded to
       BigQuery. Required unless set in the Spark configuration
       (<code>spark.conf.set(...)</code>).
   </td>
   <td>Write</td>
  </tr>
  <tr>
   <td><code>intermediateFormat</code>
   </td>
   <td>The format of the data before it is loaded to BigQuery, values can be
       either "parquet" or "orc".
       <br/>(Optional. Defaults to <code>parquet</code>). On write only.
   </td>
   <td>Write</td>
  </tr>
</table>

### Data types

With the exception of `DATETIME` and `TIME` all BigQuery data types directed map into the corresponding Spark SQL data type. Here are all of the mappings:

<!--- TODO(#2): Convert to markdown -->
<table>
  <tr>
   <td><strong>BigQuery Standard SQL Data Type </strong>
   </td>
   <td><strong>Spark SQL</strong>
<p>
<strong>Data Type</strong>
   </td>
   <td><strong>Notes</strong>
   </td>
  </tr>
  <tr>
   <td><strong><code>BOOL</code></strong>
   </td>
   <td><strong><code>BooleanType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>INT64</code></strong>
   </td>
   <td><strong><code>LongType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>FLOAT64</code></strong>
   </td>
   <td><strong><code>DoubleType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>NUMERIC</code></strong>
   </td>
   <td><strong><code>DecimalType</code></strong>
   </td>
   <td>
     This preserves <code>NUMERIC</code>'s full 38 digits of precision and 9 digits of scope.
   </td>
  </tr>
  <tr>
   <td><strong><code>STRING</code></strong>
   </td>
   <td><strong><code>StringType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>BYTES</code></strong>
   </td>
   <td><strong><code>BinaryType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>STRUCT</code></strong>
   </td>
   <td><strong><code>StructType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>ARRAY</code></strong>
   </td>
   <td><strong><code>ArrayType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>TIMESTAMP</code></strong>
   </td>
   <td><strong><code>TimestampType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>DATE</code></strong>
   </td>
   <td><strong><code>DateType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><strong><code>DATETIME</code></strong>
   </td>
   <td><strong><code>StringType</code></strong>
   </td>
   <td>Spark has no DATETIME type. Casting to TIMESTAMP uses a configured TimeZone, which defaults to the local timezone (UTC in GCE / Dataproc).
<p>
We are considering adding an optional TimeZone property to allow automatically  converting to TimeStamp, this would be consistent with Spark's handling of CSV/JSON (except they always try to convert when inferring schema, and default to the local timezone)
   </td>
  </tr>
  <tr>
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

### Filtering

The connector automatically computes column and pushdown filters the DataFrame's `SELECT` statement e.g.

```
spark.read.bigquery("publicdata:samples.shakespeare")
  .select("word")
  .where("word = 'Hamlet' or word = 'Claudius'")
  .collect()
```


filters to the column `word`  and pushed down the predicate filter `word = 'hamlet' or word = 'Claudius'`.

If you do not wish to make multiple read requests to BigQuery, you can cache the DataFrame before filtering e.g.:

```
val cachedDF = spark.read.bigquery("publicdata:samples.shakespeare").cache()
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
  .option("table", TABLE)
  .option("filter", "_PARTITIONDATE > '2019-01-01'")
  ...
  .load()
```

### Configuring Partitioning

By default the connector creates one partition per 400MB in the table being read (before filtering). This should roughly correspond to the maximum number of readers supported by the BigQuery Storage API. 
This can be configured explicitly with the <code>[parallelism](#properties)</code> property. BigQuery may limit the number of partitions based on server constraints.

## Building the Connector

The connector is built using [SBT](https://www.scala-sbt.org/). Following command creates a jar with shaded dependencies:

```
sbt assembly
```

## FAQ

### What is the Pricing for the Storage API?

See the [BigQuery pricing documentation](https://cloud.google.com/bigquery/pricing#storage-api).

### I have very few partitions

You can manually set the number of partitions with the `parallelism` property. BigQuery may provide fewer partitions than you ask for. See [Configuring Partitioning](#configuring-partitioning).

You can also always repartition after reading in Spark.

### How do I write to BigQuery?

You can use the [existing MapReduce connector](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/bigquery) or write DataFrames to GCS and then load the data into BigQuery.

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
