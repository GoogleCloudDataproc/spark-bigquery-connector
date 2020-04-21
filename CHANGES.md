# Release Notes

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
