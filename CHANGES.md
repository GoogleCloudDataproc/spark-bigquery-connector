# Release Notes

## Next
* Added new connector, `spark-4.1-bigquery` aimed to be used in Spark 4.1. Like Spark 4.1, this connector requires at
  least Java 17 runtime. It is currently in preview mode.
* PR #1452: Improved the performance of the dynamic partition overwrite for RANGE_BUCKET partitioned tables.

## 0.43.1 - 2025-10-22
* Issue #1417: Fixed ClassCastException in AWS federated identity
* PR #1432: Fixing packaging issue with the spark-4.0-bigquery connector.

## 0.43.0 - 2025-10-17
* Added new connector, `spark-4.0-bigquery` aimed to be used in Spark 4.0. Like Spark 4.0, this connector requires at
  least Java 17 runtime. It is currently in preview mode.
* PR #1367: Query Pushdown is no longer supported.
* PR #1369: Catalog enhancements
* PR #1374: Ensure TableId includes project ID if not explicitly set
* PR #1376: `materializationDataset` is now optional to read from views or queries.
* PR #1380: Fixed ImpersonatedCredentials serialization
* PR #1381: Added the option to set custom credentials scopes
* PR #1411: Added Support for [SparkSession#executeCommand](https://archive.apache.org/dist/spark/docs/3.0.0/api/java/org/apache/spark/sql/SparkSession.html#executeCommand-java.lang.String-java.lang.String-scala.collection.immutable.Map-)
* Issue #1421: Fix ArrowInputPartitionContext serialization issue. Thanks @mrjoe7 !
* BigQuery API has been upgraded to version 2.54.0
* BigQuery Storage API has been upgraded to version 3.16.1
* GAX has been upgraded to version 2.68.2
* gRPC has been upgraded to version 1.74.0

## 0.42.2 - 2025-05-16
* PR #1347: Get lineage out of query. Thanks @ddebowczyk92
* PR #1349: Add parameterized query support
* BigQuery API has been upgraded to version 2.48.1
* BigQuery Storage API has been upgraded to version 3.11.4
* GAX has been upgraded to version 2.63.1
* gRPC has been upgraded to version 1.71.0

## 0.42.1 - 2025-03-17
* CVE-2025-24970, CVE-2025-25193: Upgrading netty to version 4.1.119.Final
* PR #1284: Making BigQueryClientFactory Kryo serializable. Thanks @tom-s-powell !
* PR #1345: `materializationDataset` is no longer needed to read from views or queries.

## 0.42.0 - 2025-02-06
* PR #1333: Initial implementation of a BigQuery backed Spark Catalog
* PR #1335: Adding indirect write GCS cleanup logs
* BigQuery API has been upgraded to version 2.47.0
* BigQuery Storage API has been upgraded to version 3.11.2
* GAX has been upgraded to version 2.60.0
* Netty has been upgraded to version 4.1.117.Final
* Guava has been upgraded to version 33.4.0-jre

## 0.41.1 - 2024-12-20
* Issue #1290: Stopped using metadata for optimized count path
* Issue #1317: Improving OpenLineage 1.24.0+ compatibility
* PR #1311: Improve read session expired error message
* PR #1320: Set the `temporaryGcsBucket` to default to `fs.gs.system.bucket` if exists, negating the need to set it in Dataproc clusters.
* BigQuery API has been upgraded to version 2.45.0
* BigQuery Storage API has been upgraded to version 3.11.0
* GAX has been upgraded to version 2.59.0
* Netty has been upgraded to version 4.1.115.Final
* Arrow has been upgraded to version 17.0.0
* Avro has been upgraded to version 1.11.4

## 0.41.0 - 2024-09-05

* PR #1265 : Add additional parentheses for EqualNullSafe filter generation. Thanks @tom-s-powell !
* PR #1267 : Implement OpenLineage spark-extension-interfaces
* PR #1281 : Configure alternative BigNumeric precision and scale defaults
* Issue #1175: Add details to schema mismatch message
* BigQuery API has been upgraded to version 2.42.2
* BigQuery Storage API has been upgraded to version 3.9.0
* GAX has been upgraded to version 2.52.0
* Netty has been upgraded to version 4.1.113.Final

## 0.40.0 - 2024-08-05

* PR #1259 : Encode snapshotTimeMillis in view materialization query. Thanks @tom-s-powell !
* PR #1261 : Adding IdentityToken header in readRows call
* Issue #1043 : Fix Indirect write drops policy tags
* Issue #1244 : Set schema Field Nullables as per ALLOW_FIELD_RELAXATION
* Issue #1254 : fix getting partitioning fields for pseudo columns
* Issue #1263 : Support ClusteredFields in Direct Write
* BigQuery API has been upgraded to version 2.42.0
* BigQuery Storage API has been upgraded to version 3.8.0
* GAX has been upgraded to version 2.51.0
* gRPC has been upgraded to version 1.65.1
* Netty has been upgraded to version 4.1.112.Final


## 0.39.1 - 2024-06-24

* PR #1236: Fixing unshaded artifacts, added shading verification
* PR #1239: Allow GCS bucket to be supplied including a scheme. Thanks @tom-s-powell !
* Issue #1126: Fixing Kryo serialization issues
* Issue #1223: Fix gRPC status creates 'object not serializable' errors

## 0.39.0 - 2024-05-21

* PR #1221: Adding support for [table snapshots](https://cloud.google.com/bigquery/docs/table-snapshots-intro). Thanks @tom-s-powell !
* PR #1222: Add option to request lz4 compressed ReadRowsResponse
* PR #1225: Fixing multi-release jar shading
* PR #1227: Optimizing dynamic partition overwrite for Time partitioned tables
* BigQuery API has been upgraded to version 2.40.1
* BigQuery Storage API has been upgraded to version 3.5.1
* GAX has been upgraded to version 2.48.1
* gRPC has been upgraded to version 1.64.0

## 0.38.0 - 2024-05-01

* PR #1205: Sending Identity token in the read API header
* Issue #1195: Support map type with complex value
* Issue #1215: Support predicate pushdown for DATETIME
* BigQuery API has been upgraded to version 2.39.0
* BigQuery Storage API has been upgraded to version 3.5.0
* GAX has been upgraded to version 2.47.0
* Arrow has been upgraded to version 16.0.0
* gRPC has been upgraded to version 1.63.0
* Netty has been upgraded to version 4.1.109.Final

## 0.37.0 - 2024-03-25

* :warning: Starting version 0.38.0 of the connector, the `spark-2.4-bigquery` version won't be released as Spark 2.4 is
  well-supported by the `spark-bigquery-with-dependencies` connectors.
* PR #1156: Propagate stats for BigLake Managed tables
* PR #1181: Add caching during protobuf generation
* PR #1190: Enable connection sharing for atLeastOnce writes
* Issue #1182: Fix query check logic
* BigQuery API has been upgraded to version 2.38.1
* BigQuery Storage API has been upgraded to version 3.3.1
* GAX has been upgraded to version 2.45.0
* Arrow has been upgraded to version 15.0.1
* gRPC has been upgraded to version 1.62.2
* Netty has been upgraded to version 4.1.107.Final
* Protocol Buffers has been upgraded to version 3.25.3


## 0.36.1 - 2024-01-31

* PR #1176: fix timestamp filter translation issue

## 0.36.0 - 2024-01-25

* PR #1155: allow lazy materialization of query on load
* PR #1163: Added config to set the BigQuery Job timeout
* PR #1166: Fix filters by adding surrounding parenthesis. Thanks @tom-s-powell !
* PR #1171: fix read, write issues with Timestamp
* Issue #1116: BigQuery write fails with MessageSize is too large
* BigQuery API has been upgraded to version 2.36.0
* GAX has been upgraded to version 2.40.0
* gRPC has been upgraded to version 1.61.0
* Netty has been upgraded to version 4.1.106.Final
* Protocol Buffers has been upgraded to version 3.25.2

## 0.35.1 - 2023-12-28

* PR #1153: allow writing spark string to BQ datetime

## 0.35.0 - 2023-12-19

* PR #1115: Added new connector, `spark-3.5-bigquery` aimed to be used in Spark 3.5. This connector implements new APIs and capabilities provided by the Spark Data Source V2 API.
* PR #1117: Make read session caching duration configurable
* PR #1118: Improve read session caching key
* PR #1122: Set traceId on write
* PR #1124: Added `SparkListenerEvent`s for Query and Load jobs running on BigQuery
* PR #1127: Fix job labeling for mixed case Dataproc job names
* PR #1136: Consider projections for biglake stats
* PR #1143: Enable async write for default stream
* BigQuery API has been upgraded to version 2.35.0
* BigQuery Storage API has been upgraded to version 2.47.0
* GAX has been upgraded to version 2.38.0
* gRPC has been upgraded to version 1.60.0
* Netty has been upgraded to version 4.1.101.Final
* Protocol Buffers has been upgraded to version 3.25.1

## 0.34.0 - 2023-10-31

* PR #1057: Enable async writes for greater throughput
* PR #1094: CVE-2023-5072: Upgrading the org.json:json dependency
* PR #1095: CVE-2023-4586: Upgrading the netty dependencies
* PR #1104: Fixed nested field predicate pushdown
* PR #1109: Enable read session caching by default for faster Spark planning
* PR #1111: Enable retry of failed messages
* Issue #103: Support for Dynamic partition overwrite for time and range partitioned table
* Issue #1099: Fixing the usage of ExternalAccountCredentials
* BigQuery API has been upgraded to version 2.33.2
* BigQuery Storage API has been upgraded to version 2.44.0
* GAX has been upgraded to version 2.35.0
* gRPC has been upgraded to version 1.58.0
* Protocol Buffers has been upgraded to version 3.24.4

## 0.33.0 - 2023-10-17

* Added new connector, `spark-3.4-bigquery` aimed to be used in Spark 3.4 and above. This connector implements new APIs and capabilities provided by the Spark Data Source V2 API.
* PR #1008: Adding support to expose BigQuery metrics using Spark custom metrics API.
* PR #1038: Logical plan now shows the BigQuery table of DirectBigQueryRelation. Thanks @idc101 !
* PR #1058: View names will appear in query plan instead of the materialized table
* PR #1061: Handle NPE case when reading BQ table with NUMERIC fields. Thanks @hayssams !
* PR #1069: Support TimestampNTZ datatype in spark 3.4
* Issue #453: fix comment handling in query
* Issue #144: allow writing Spark String to BQ TIME type
* Issue #867: Support writing with RangePartitioning
* Issue #1046: Add a way to disable map type support
* Issue #1062: Adding dataproc job ID and UUID labels to BigQuery jobs

## 0.32.2 - 2023-08-07

* CVE-2023-34462: Upgrading netty to verision 4.1.96.Final

## 0.32.1 - 2023-08-03

* PR #1025: Handle Java 8 types for dates and timestamps when compiling filters. Thanks @tom-s-powell !
* Issue #1026: Fixing Numeric conversion
* Issue #1028: Fixing PolicyTags removal on overwrite

## 0.32.0 - 2023-07-17

* Issue #748: `_PARTITIONDATE` pseudo column is provided only for ingestion time **daily** partitioned tables
* Issue #990: Fix to support `allowFieldAddition` for columns with nested fields.
* Issue #993: Spark ML vector read and write fails
* PR #1007: Implement at-least-once option that utilizes default stream

## 0.31.1 - 2023-06-06

* Issue #988: Read statistics are logged at TRACE level. Update the log4j configuration accordingly in order to log them.

## 0.31.0 - 2023-06-01

* :warning: **Breaking Change** BigNumeric conversion has changed, and it is now converted to Spark's
  Decimal data type. Notice that BigNumeric can have a wider precision than Decimal, so additional
  setting may be needed. See [here](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#numeric-and-bignumeric-support)
  for additional details.
* Issue #945: Fixing unable to add new column even with option `allowFieldAddition`
* PR #965: Fix to reuse the same BigQueryClient for the same BigQueryConfig, rather than creating a new one
* PR #950: Added support for service account impersonation
* PR #960: Added support for basic configuration of the gRPC channel pool size in the BigQueryReadClient.
* PR #973: Added support for writing to [CMEK managed tables](https://cloud.google.com/bigquery/docs/customer-managed-encryption).
* PR #971: Fixing wrong results or schema error when Spark nested schema pruning is on for datasource v2
* PR #974: Applying DPP to Hive partitioned BigLake tables (spark-3.2-bigquery and spark-3.3-bigquery only)
* PR #986: CVE-2020-8908, CVE-2023-2976: Upgrading Guava to version 32.0-jre
* BigQuery API has been upgraded to version 2.26.0
* BigQuery Storage API has been upgraded to version 2.36.1
* GAX has been upgraded to version 2.26.0
* gRPC has been upgraded to version 1.55.1
* Netty has been upgraded to version 4.1.92.Final
* Protocol Buffers has been upgraded to version 3.23.0
* PR #957: support direct write with subset field list.

## 0.30.0 - 2023-04-11

* New connectors are out of preview and are now generally available! This includes all the new
  connectors: spark-2.4-bigquery, spark-3.1-bigquery, spark-3.2-bigquery and spark-3.3-bigquery are GA and ready to be used in all workloads. Please
  refer to the [compatibility matrix](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#connector-to-spark-compatibility-matrix)
  when using them.
* Direct write method is out of preview and is now generally available!
* `spark-bigquery-with-dependencies_2.11` is no longer published. If a recent version of the Scala
  2.11 connector is needed, it can be built by checking out the code and running
  `./mvnw install -Pdsv1_2.11`.
* Issue #522: Supporting Spark's Map type. Notice there are few restrictions as this is not a
  BigQuery native type.
* Added support for reading BigQuery table snapshots.
* BigQuery API has been upgraded to version 2.24.4
* BigQuery Storage API has been upgraded to version 2.34.2
* GAX has been upgraded to version 2.24.0
* gRPC has been upgraded to version 1.54.0
* Netty has been upgraded to version 4.1.90.Final
* PR #944: Added support to set query job priority
* Issue #908: Making sure that `preferred_min_stream_count` must be less than or equal to `max_stream_count`

## 0.29.0 - 2023-03-03

* Added two new connectors, `spark-3.2-bigquery` and `spark-3.3-bigquery` aimed to be used in Spark 3.2 and 3.3
  respectively. Those connectors implement new APIs and capabilities provided by the Spark Data Source V2 API. Both
  connectors are in preview mode.
* Dynamic partition pruning is supported in preview mode by `spark-3.2-bigquery` and `spark-3.3-bigquery`.
* This is the last version of the Spark BigQuery connector for scala 2.11. The code will remain in the repository and
  can be compiled into a connector if needed.
* PR #857: Fixing `autovalue` shaded classes repackaging
* BigQuery API has been upgraded to version 2.22.0
* BigQuery Storage API has been upgraded to version 2.31.0
* GAX has been upgraded to version 2.23.0
* gRPC has been upgraded to version 1.53.0
* Netty has been upgraded to version 4.1.89.Final

## 0.28.1 - 2023-02-27

PR #904: Fixing premature client closing in certain cases, which causes RejectedExecutionException to be thrown

## 0.28.0 - 2023-01-09

* Adding support for the [JSON](https://cloud.google.com/bigquery/docs/reference/standard-sql/json-data) data type.
  Thanks to @abhijeet-lele and @jonathan-ostrander for their contributions!
* Issue #821: Fixing direct write of empty DataFrames
* PR #832: Fixed client closing
* Issue #838: Fixing unshaded artifacts
* PR #848: Making schema comparison on write less strict
* PR #852: fixed `enableListInference` usage when using the default intermediate format
* Jackson has been upgraded to version 2.14.1, addressing CVE-2022-42003
* BigQuery API has been upgraded to version 2.20.0
* BigQuery Storage API has been upgraded to version 2.27.0
* GAX has been upgraded to version 2.20.1
* Guice has been upgraded to version 5.1.0
* gRPC has been upgraded to version 1.51.1
* Netty has been upgraded to version 4.1.86.Final
* Protocol Buffers has been upgraded to version 3.21.12

## 0.27.1 - 2022-10-18

* PR #792: Added ability to set table labels while writing to a BigQuery table
* PR #796: Allowing custom BigQuery API endpoints
* PR #803: Removed grpc-netty-shaded from the connector jar
* Protocol Buffers has been upgraded to version 3.21.7, addressing CVE-2022-3171
* BigQuery API has been upgraded to version 2.16.1
* BigQuery Storage API has been upgraded to version 2.21.0
* gRPC has been upgraded to version 1.49.1
* Netty has been upgraded to version 4.1.82.Final

## 0.27.0 - 2022-09-20

* Added new Scala 2.13 connector, aimed at Spark versions from 3.2 and above
* PR #750: Adding support for custom access token creation. See more [here](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#how-do-i-authenticate-outside-gce--dataproc).
* PR #745: Supporting load from query in spark-3.1-bigquery.
* PR #767: Adding the option createReadSessionTimeoutInSeconds, to override the timeout for CreateReadSession.

## 0.26.0 - 2022-07-18

* All connectors support the DIRECT write method, using the BigQuery Storage Write API,
  without first writing the data to GCS. **DIRECT write method is in preview mode**.
* `spark-3.1-bigquery` has been released in preview mode. This is a Java only library,
  implementing the Spark 3.1 DataSource v2 APIs.
* BigQuery API has been upgraded to version 2.13.8
* BigQuery Storage API has been upgraded to version 2.16.0
* gRPC has been upgraded to version 1.47.0
* Netty has been upgraded to version 4.1.79.Final

## 0.25.2 - 2022-06-22

* PR #673: Added integration tests for BigLake external tables.
* PR #674: Increasing default maxParallelism to 10K for BigLake external tables

## 0.25.1 - 2022-06-13

* Issue #651: Fixing the write back to BigQuery.
* PR #664: Add support for BigLake external tables.
* PR #667: Allowing clustering on unpartitioned tables.
* PR #668: Using spark default parallelism as default.

## 0.25.0 - 2022-05-31
* Issue #593: Allow users to disable cache when loading data via SQL query,
  by setting `cacheExpirationTimeInMinutes=0`
* PR #613: Added field level schema checks. This can be disabled by setting
  `enableModeCheckForSchemaFields=false`
* PR #618: Added support for the `enableListInterface` option. This allows to
  use parquet as an intermediate format also for arrays, without adding the
  `list` element in the resulting schema as described
  [here](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ParquetOptions)
* PR #641: Removed Conscrypt from the shaded artifact in order to improve
  compatibility with Dataproc Serverless and with clusters where Conscrypt is
  disabled.
* BigQuery API has been upgraded to version 2.10.6
* BigQuery Storage API has been upgraded to version 2.12.0
* gRPC has been upgraded to version 1.46.0
* Netty has been upgraded to version 4.1.75.Final

## 0.24.2 - 2022-04-05
* PR #580: Fixed shaded artifacts version flattening, the version appears
  correctly in the released POM
* PR #583: netty-tcnative is taken from the Netty BOM
* PR #584: CVE-2020-36518 - Upgraded jackson

## 0.24.1 - 2022-03-29
* PR #576: Fixed error running on Datapoc clusters where conscrypt is disabled
  (the property`dataproc.conscrypt.provider.enable` set to `false`)

## 0.24.0 - 2022-03-23
* Issue #530: Treating Field.mode==null as Nullable
* PR #518: Cache expiration time can be configured now.
* PR #561: Added support for adding trace ID to the BigQuery reads and writes.
  The trace Id will be of the format `Spark:ApplicateName:JobID`. The
  application name must be set by the user, job ID is defaults to Dataproc job
  ID if exists, otherwise it is set to `spark.app.id`.
* PR #563: Fixed a bug where using writeMethod=DIRECT and SaveMode=Append the
  destination table may have been deleted in case `abort()` has been called.
* PR #568: Added support for BigQuery jobs labels
* BigQuery API has been upgraded to version 2.9.4
* BigQuery Storage API has been upgraded to version 2.11.0
* gRPC has been upgraded to version 1.44.1
* Netty has been upgraded to version 4.1.73.Final

## 0.23.2 - 2022-01-19
* PR #521: Added Arrow compression options to the
  spark-bigquery-with-dependencies_2.* connectors
* PR #526: Added the option to use parent project for the metadata/jobs API as
  well
* BigQuery API has been upgraded to version 2.3.3
* BigQuery Storage API has been upgraded to version 2.4.2
* gRPC has been upgraded to version 1.42.1
* Netty has been upgraded to version 4.1.70.Final

## 0.23.1 - 2021-12-08
* Issue #501: Fixed using avro as an intermediate type for writing.

## 0.23.0 - 2021-12-06
* New connector: A Java only connector implementing the Spark 2.4 APIs
* PR #469: Added support for the BigQuery Storage Write API, allowing faster
  writes (Spark 2.4 connector only)
* Issue #481: Added configuration option to use compression from the READ API
  for Arrow
* BigQuery API has been upgraded to version 2.1.8
* BigQuery Storage API has been upgraded to version 2.1.2
* gRPC has been upgraded to version 1.41.0

## 0.22.2 - 2021-09-22
* Issue #446: BigNumeric values are properly written to BigQuery
* Issue #452: Adding the option to clean BigQueryClient.destinationTableCache
* BigQuery API has been upgraded to version 2.1.12
* BigQuery Storage API has been upgraded to version 2.3.1
* gRPC has been upgraded to version 1.40.0

## 0.22.1 - 2021-09-08
* Issue #444: allowing unpartitioned clustered table

## 0.22.0 - 2021-06-22
* PR #404: Added support for BigNumeric
* PR #430: Added HTTP and gRPC proxy support
* Issue #273: Resolved the streaming write issue for spark 3.x

## 0.21.1 - 2021-06-22
* PR #413: Pushing all filters to BigQuery Storage API
* Issue #412: Supporting WITH queries
* Issue #409: Allowing all whitespaces after the select
* PR #419: Fix a bug where background threads > 2 cases would miss pages (DSv2)
* PR #416: Moved zstd-jni library to be provided in order to solve Spark 2.4 compatibility (DSv2)
* PR #417: Added back column projection to DSv2

## 0.21.0 - 2021-06-01
* Issue #354: users can query a view with different columns in select() and filter()
* Issue #367: Struct column order is fixed
* Issue #383: Fixed table metadata update when writing to a partitioned table
* Issue #390: Allowing additional white-space types in the query
* Issue #393: replacing avro.shaded dependency with guava
* PR #360: Removed redundant `UNNEST` when compiling `IN` condition
* BigQuery API has been upgraded to version 1.131.1
* BigQuery Storage API has been upgraded to version 1.22.0
* Guava has been upgraded to version 30.1.1-jre
* gRPC has been upgraded to version 1.37.1
* Netty has been upgraded to version 4.1.65.Final

## 0.20.0 - 2021-03-29
* PR #375: Added support for pseudo column support - time partitioned table now supoort the _PARTITIONTIME and _PARTITIONDATE fields
* Issue# 190: Writing data to BigQuery properly populate the field description
* Issue #265: Fixed nested conjunctions/disjunctions when using the AVRO read format
* Issue #326: Fixing netty_tcnative_windows.dll shading
* Arrow has been upgraded to version 4.0.0

## 0.19.1 - 2021-03-01
* PR #324 - Restoring version 0.18.1 dependencies due to networking issues
* BigQuery API has been upgraded to version 1.123.2
* BigQuery Storage API has been upgraded to version 1.6.0
* Guava has been upgraded to version 30.0-jre
* Netty has been upgraded to version 4.1.51.Final

## 0.19.0 - 2021-02-24
* Issue #247: Allowing to load results of any arbitrary SELECT query from BigQuery.
* Issue #310: Allowing to configure the expiration time of materialized data.
* PR #283: Implemented Datasource v2 write support.
* Improved Spark 3 compatibility.
* BigQuery API has been upgraded to version 1.127.4
* BigQuery Storage API has been upgraded to version 1.10.0
* Guava has been upgraded to version 30.1-jre
* Netty has been upgraded to version 4.1.52.Final

## 0.18.1 - 2021-01-21
* Issue #248: Reducing the size of the URI list when writing to BigQuery. This allows larger DataFrames (>10,000 partitions) to be safely written.
* Issue #296: Removed redundant packaged slf4j-api.
* PR #276: Added the option to enable `useAvroLogicalTypes` option When writing data to BigQuery.

## 0.18.0 - 2020-11-12
* Issue #226: Adding support for HOUR, MONTH, DAY TimePartitions
* Issue #260: Increasing connection timeout to the BigQuery service, and
  configuring the request retry settings.
* Issue #263: Fixed `select *` error when ColumnarBatch is used (DataSource v2)
* Issue #266: Fixed the external configuration not working regression bug
  (Introduced in version 0.17.2)
* PR #262: Filters on BigQuery DATE and TIMESTAMP now use the right type.
* BigQuery API has been upgraded to version 1.123.2
* BigQuery Storage API has been upgraded to version 1.6.0
* Guava has been upgraded to version 30.0-jre
* Netty has been upgraded to version 4.1.51.Final
* netty-tcnative has been upgraded to version 4.1.34.Final

## 0.17.3 - 2020-10-06
* PR #242, #243: Fixed Spark 3 compatibility, added Spark 3 acceptance test
* Issue #249: Fixing credentials creation from key

## 0.17.2 - 2020-09-10
* PR #239: Ensuring that the BigQuery client will have the proper project id

## 0.17.1 - 2020-08-06
* Issue #216: removed redundant ALPN dependency
* Issue #219: Fixed the LessThanOrEqual filter SQL compilation in the DataSource v2 implmentation
* Issue #221: Fixed ProtobufUtilsTest.java with newer BigQuery dependencies
* PR #229: Adding support for Spark ML Vector and Matrix data types
* BigQuery API has been upgraded to version 1.116.8
* BigQuery Storage API has been upgraded to version 1.3.1

## 0.17.0 - 2020-07-15
* PR #201: [Structured streaming write](http://spark.apache.org/docs/2.4.5/structured-streaming-programming-guide.html#starting-streaming-queries)
  is now supported (thanks @varundhussa)
* PR #202: Users now has the option to keep the data on GCS after writing to BigQuery (thanks @leoneuwald)
* PR #211: Enabling to overwrite data of a single date partition
* PR #198: Supporting columnar batch reads from Spark in the DataSource V2 implementation. **It is not ready for production use.**
* PR #192: Supporting `MATERIALIZED_VIEW` as table type
* Issue #197: Conditions on StructType fields are now handled by Spark and not the connector
* BigQuery API has been upgraded to version 1.116.3
* BigQuery Storage API has been upgraded to version 1.0.0
* Netty has been upgraded to version 4.1.48.Final (Fixing issue #200)

## 0.16.1 - 2020-06-11
* PR #186: Fixed SparkBigQueryConnectorUserAgentProvider initialization bug

## 0.16.0 - 2020-06-09
**Please don't use this version, use 0.16.1 instead**

* PR #180: Apache Arrow is now the default read format. Based on our benchmarking, Arrow provides read
  performance faster by 40% then Avro.
* PR #163: Apache Avro was added as a write intermediate format. It shows better performance over parquet
  in large (>50GB) datasets. The spark-avro package must be added in runtime in order to use this format.
* PR #176: Usage simplification: Now instead of using the `table` mandatory option, user can use the built
  in `path` parameter of `load()` and `save()`, so that read becomes
  `df = spark.read.format("bigquery").load("source_table")` and write becomes
  `df.write.format("bigquery").save("target_table")`
* An experimental implementation of the DataSource v2 API has been added. **It is not ready for
  production use.**
* BigQuery API has been upgraded to version 1.116.1
* BigQuery Storage API has been upgraded to version 0.133.2-beta
* gRPC has been upgraded to version 1.29.0
* Guava has been upgraded to version 29.0-jre

## 0.15.1-beta - 2020-04-27
* PR #158: Users can now add the `spark.datasource.bigquery` prefix to the configuration options in order to support Spark's `--conf` command line flag
* PR #160: View materialization is performed only on action, fixing a bug where view materialization was done too early

## 0.15.0-beta - 2020-04-20
* PR #150: Reading `DataFrame`s should be quicker, especially in interactive usage such in notebooks
* PR #154: Upgraded to the BigQuery Storage v1 API
* PR #146: Authentication can be done using [AccessToken](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/print-access-token)
  on top of Credentials file, Credentials, and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

## 0.14.0-beta - 2020-03-31
* Issue #96: Added Arrow as a supported format for reading from BigQuery
* Issue #130 Adding the field description to the schema metadata
* Issue #124: Fixing null values in ArrayType
* Issue #143: Allowing the setting of `SchemaUpdateOption`s When writing to BigQuery
* PR #148: Add support for writing clustered tables
* Upgrade version of google-cloud-bigquery library to 1.110.0
* Upgrade version of google-cloud-bigquerystorage library to 0.126.0-beta


## 0.13.1-beta - 2020-02-14
* The BigQuery Storage API was reverted to v1beta1. The v1beta2 API has not been
  fully integrated with custom IAM roles, which can cause issues to customers using
  those. The v1beta1 doesn't have this problem. Once the integration is complete,
  the API will be upgraded again.

## 0.13.0-beta - 2020-02-12
**Please don't use this version, use 0.13.1-beta instead**

* Moved to use BigQuery Storage API v1beta2
* changed the `parallelism` parameter to `maxParallelism` in order to reflect the
  Change in the underlining API (the old parameter has been deprecated)
* Upgrade version of google-cloud-bigquerystorage library to 0.122.0-beta.
* Issue #73: Optimized empty projection used for count() execution.
* Issue #121: Added the option to configure CreateDisposition when inserting data
  to BigQuery.

## 0.12.0-beta - 2020-01-29
* Issue #72: Moved the shaded jar name from classifier to a new artifact name
* Issues #73, #87: Added better logging to help understand which columns and filters
  are asked by spark, and which are passed down to BigQuery
* Issue #107: The connector will now alert when is is used with the wrong scala version

## 0.11.0-beta - 2019-12-18
* Upgrade version of google-cloud-bigquery library to 1.102.0
* Upgrade version of google-cloud-bigquerystorage library to 0.120.0-beta
* Issue #6: Do not initialize bigquery options by default
* Added ReadRows retries on GRPC internal errors
* Issue #97: Added support for GEOGRAPHY type

## 0.10.0-beta - 2019-11-14
* Added preliminary support for reading from BigQuery views (Issue #21)
* Writing to BigQuery now white-listing the intermediate files instead
  of black listing the _SUCCESS files (PR #75)
* Added count() tip to the README

## 0.9.2-beta - 2019-11-11
* Upgrade version of google-cloud-bigquery library to 1.99.0
* Upgrade version of google-cloud-bigquerystorage library to 0.117.0-beta
* Upgrade version of grpc-netty-shaded library to 1.24.1
* Supporting reading large rows (Issue #22, https://issuetracker.google.com/143730055)
* Made sure that all filters are pushed down (Issue #74)
* Fixing log severity
* Added Java Example

## 0.9.1-beta - 2019-10-11
* A NPE in the shutdown hook has occurred in case the delete had succeeded
  in the first time. This had no impact on the actual logic, just on the log.
  The method now verifies the path exists before trying to delete it, and
  hides the redundant exception.
* Added support for data.write.bigquery("table") implicit import, fixed
  regression caused by relying of shaded scalalogging

## 0.9.0-beta - 2019-10-08
* Added write support
* Switch requested partitions from SparkContext.defaultParallelism to one
  partition per 400MB. This should work better with balanced sharding and
  dynamic allocation.
* Cross built for both Scala 2.11 and 2.12
* Upgrade version of google-cloud-bigquery library to 1.96.0
* Upgrade version of google-cloud-bigquerystorage library to 0.114.0-beta
* Upgrade version of grpc-netty-shaded library to 1.23.0

## 0.8.1-beta - 2019-09-12
* Added a shaded version

## 0.8.0-beta - 2019-07-22
* Upgrade version of google-cloud-bigquery library to 1.82.0
* Upgrade version of google-cloud-bigquerystorage library to 0.98.0-beta
* Upgrade version of grpc-netty-shaded library to 1.22.1
* Use balanced sharding strategy to assign roughly same number of rows to each
  read stream.
* Update filtering support to reflect full set of filters supported by storage
  API (multi-clause predicates, pseudo-column variables, additional filter
  clauses - IsNull, IsNotNull, In, StringStartsWith, StringEndsWith,
  StringContains etc.)

## 0.7.0-beta - 2019-06-26
* Switch to using the BALANCED sharding strategy, which balances work between
  streams on the server-side, leading to a more uniform distribution of rows
  between partitions

## 0.6.0-beta - 2019-06-25
* Support specifying credentials through configurations

## 0.5.1-beta - 2019-04-26

* Support Numeric type
* Refactor tests to manage test datasets

## 0.5.0-beta - 2019-03-06

* Initial release
