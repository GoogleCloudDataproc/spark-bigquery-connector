# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows [Google's Open Source Community
Guidelines](https://opensource.google.com/conduct/).

## Building and Testing the Connector

The connector is built using the Maven wrapper. The project contains several
connectors, sharing some of the code among them. The connectors are:
* spark-bigquery_2.11 - a Scala 2.11 based connector, targeting Spark 2.3 and
  2.4 using Scala 2.11.
* spark-bigquery_2.12 - a Scala 2.12 based connector, targeting Spark 2.4 and
  3.x using Scala 2.12.
* spark-2.4-bigquery - a Java only connector, targeting Spark 2.4 (of all
  Scala versions), using the new DataSource APIs.
* spark-3.1-bigquery - a Java only connector, targeting Spark 3.1 (of all
  Scala versions), using the new DataSource APIs. Still under development.

The project's artifacts are:

* `spark-bigquery-parent` - The parent POM for all artifacts. Common settings
  and artifact version should be defined here.
* `bigquery-connector-common` - Utility classes for working with the BigQuery
  APIs. This artifact has no dependency on Spark. This artifact can potentially
  be used by non-spark connectors
* `spark-bigquery-connector-common`- Common utilites and logic  shared among
  all connectors (Scala and Java alike). Whenever possible, new code should be
  in this artifact
* `spark-bigquery-dsv1/spark-bigquery-dsv1-parent` - Common settings for the
  Scala based DataSource V1 implementation.
* `spark-bigquery-dsv1/spark-bigquery-dsv1-spark3-support` - As some of the APIs
  used by the connector have changed between Spark 2.x and 3.x, they are wrapped
  in a neutral interface with wrappers for the two implementations. Unlike the
  other dsv1 artifacts, this project depends on Spark 3 for this reason.
* `spark-bigquery-dsv1/spark-bigquery_2.11` and
  `spark-bigquery-dsv1/spark-bigquery_2.12` - The implementation of the Scala
  based connectors. Both connectors share the same code via a symbolic link, so
  a change in one of them will automatically affect the other.
* `spark-bigquery-dsv1/spark-bigquery-with-dependencies-parent` - Common
  settings for the shaded artifacts.
* `spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.11` and
  `spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.12` - The shaded
  distributable of the connector, containing all the dependencies.
* `spark-bigquery-dsv2/spark-bigquery-dsv2-parent` - Common settings for the
  Java only DataSource V2 implementations.
* `spark-bigquery-dsv2/spark-2.4-bigquery` - A Java only DataSource V2
  connector implementing the Spark 2.4 APIs.
* `spark-bigquery-dsv2/spark-3.1-bigquery` - A Java only DataSource V2
  connector implementing the Spark 3.1 APIs. Under development.
* `spark-bigquery-python-lib` - The python support library, adding BigQuery
  types not supported by Spark.

As building and running all the connectors is a lengthy process, the project is
split into several [profiles](https://maven.apache.org/guides/introduction/introduction-to-profiles.html),
each building only a subset of the project's artifacts. The profiles are:

* `dsv1` - Running both Scala/DSv1 connectors.
* `dsv1_2.11` - Running just the Scala 2.11 connector.
* `dsv1_2.12` - Running just the Scala 2.12 connector.
* `dsv2` - Running both Java/DSv2 connectors.
* `dsv2_2.4` - Running just the Java Spark 2.4 connector.
* `dsv2_3.1` - Running just the Java Spark 3.1 connector.
* `all` - Running all the connectors.

Example: In order to compile **just** the Scala 2.12 connector run
`./mvnw install -Pdsv1_2.12`.

**Note**: Need java 1.8 and make sure **/usr/libexec/java_home** set to java 1.8 before building any module.

**Important**: If no profile is selected, then only the common artifacts are run.

The integration and acceptance tests are disabled by default. In order to run it please add the
following profiles to the run:
* Integration tests - `./mvnw failsafe:integration-test -Pdsv1_2.11,integration`
* Acceptance tests - `./mvnw verify -Pdsv2_2.4,acceptance`

In order to run the integration tests make sure that your GCP user has the proper accounts for creating and deleting
datasets and tables in your test project in BigQuery. It will also need the permissions to upload files to the test
bucket in GCS as well as delete them.

Setting the following environment variables is required to run the integration tests:
* `GOOGLE_APPLICATION_CREDENTIALS` - the full path to a credentials JSON, either a service account or the result of a
  `gcloud auth login` run
* `GOOGLE_CLOUD_PROJECT` - The Google cloud platform project used to test the connector
* `TEMPORARY_GCS_BUCKET` - The GCS bucked used to test writing to BigQuery during the integration tests
* `ACCEPTANCE_TEST_BUCKET` - The GCS bucked used to test writing to BigQuery during the acceptance tests
* `SERVERLESS_NETWORK_URI` - The network used by the serverless batches during the acceptance tests
* `BIGLAKE_CONNECTION_ID` - The connection ID to create a BigLake table using a Cloud Resource connection
