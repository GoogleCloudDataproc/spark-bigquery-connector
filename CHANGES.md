# Release Notes

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
