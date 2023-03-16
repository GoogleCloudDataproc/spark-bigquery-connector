# Release Notes

## Next

* Issue #522: Supporting Spark's Map type. Notice there are few restrictions as this is not a
  BigQuery native type.

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
* Issue #908: Making sure that `preferred_min_stream_count` must be less than or equal to `max_stream_count`

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
