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

The connector is build using sbt version 0.13. The following targets are used:
* `sbt publishM2` - builds the connector and publishes it to the local maven repository
* `sbt test` - runs the unit tests
* `sbt it:test` - runs the integration tests. Those tests run various cases using a GCP project used for testing.
* `sbt acceptance:test` - runs the acceptance test. Those test create several Dataproc clusters and then run several
  test scripts using PySpark. Please run `sbt publishM2` before in order to build the tested artifacts.
  
In order to run the integration tests make sure that your GCP user has the proper accounts for creating and deleting
datasets and tables in your test project in BigQuery. It will also need the permissions to upload files to the test
bucket in GCS as well as delete them.

Setting the following environment variables is requitred to run the integration tests:
* `GOOGLE_APPLICATION_CREDENTIALS` - the full path to a credentials JSON, either a service account or the result of a
  `gcloud auth login` run
* `GOOGLE_CLOUD_PROJECT` - The Google cloud platform project used to test the connector
* `TEMPORARY_GCS_BUCKET` - The GCS bucked used to test writing to BigQuery during the integration tests
* `ACCEPTANCE_TEST_BUCKET` - The GCS bucked used to test writing to BigQuery during the acceptance tests 
