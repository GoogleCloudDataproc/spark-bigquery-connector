# Release Notes

## 0.9.0-beta - 2019-XX-XX

* Switch requested partitions from SparkContext.defaultParallelism to one
  partition per 400MB. This should work better with balanced sharding and
  dynamic allocation.

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
* Support specifiying credentials through configurations

## 0.5.1-beta - 2019-04-26

* Support Numeric type
* Refactor tests to manage test datasets

## 0.5.0-beta - 2019-03-06

* Initial release
