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

The connector is built using the Maven wrapper (`./mvnw`). The repository
publishes several connector jars sharing a common core, organized along three
axes: API generation (DSv1 / DSv2), Spark version, and Scala version.

> **Source of truth for the test commands CI runs:**
> [`cloudbuild/presubmit.sh`](cloudbuild/presubmit.sh) (presubmit: init, unit
> tests, integration tests sharded per profile) and
> [`cloudbuild/nightly.sh`](cloudbuild/nightly.sh) (nightly: above plus
> acceptance tests). If the examples below ever drift, those scripts win.

### Modules

Always built (no profile required):

* `spark-bigquery-parent` — parent POM with shared versions, plugin config, and
  the `integration` / `acceptance` profile definitions.
* `bigquery-connector-common` — pure BigQuery API utilities, no Spark
  dependency. Reusable by non-Spark callers.
* `spark-bigquery-connector-common` — Spark-aware logic shared between DSv1 and
  DSv2. Whenever possible, **new shared code should land here.**
* `spark-bigquery-tests` — shared integration-test fixtures.
* `spark-bigquery-scala-212-support` — Scala 2.12 compatibility shims.
* `spark-bigquery-python-lib` — Python helpers for BigQuery types not native
  to Spark.

DSv1 (Scala) connectors under `spark-bigquery-dsv1/`:

* `spark-bigquery-dsv1-parent`, `spark-bigquery-dsv1-spark2-support`,
  `spark-bigquery-dsv1-spark3-support` — sub-parent and Spark-version API
  shims.
* `spark-bigquery_2.12`, `spark-bigquery_2.13` — Scala 2.12 and 2.13
  connectors. The two modules keep **near-duplicate** `src/main` and
  `src/test` trees (only the per-Scala-version provider differs). Apply
  non-Scala-specific changes to both. Only the shared build configuration
  under `src/build` is symlinked, pointing each module at
  `spark-bigquery-dsv1/src/build/`.
* `spark-bigquery-with-dependencies_2.12`,
  `spark-bigquery-with-dependencies_2.13` — the shaded distributable jars
  bundling all dependencies.

DSv2 (Java) connectors under `spark-bigquery-dsv2/`:

* `spark-bigquery-dsv2-parent`, `spark-bigquery-dsv2-common`,
  `spark-bigquery-metrics` — shared infrastructure.
* `spark-3.1-bigquery`, `spark-3.2-bigquery`, `spark-3.3-bigquery`,
  `spark-3.4-bigquery`, `spark-3.5-bigquery`, `spark-4.0-bigquery`,
  `spark-4.1-bigquery` — the per-Spark-version connector jars, each backed by
  a `*-bigquery-lib` module reused by the next-version connector.

Pushdown variants under `spark-bigquery-pushdown/` add query pushdown support
per Spark+Scala combination. They are built separately and are **not** driven
by the root `dsv1*` / `dsv2*` profiles.

Other:

* `coverage` — Jacoco aggregator, activated by `-Pcoverage`.
* `examples`, `scripts` — sample code and helper scripts (not Maven modules).

A few legacy modules remain on disk (`spark-bigquery_2.11`,
`spark-bigquery-with-dependencies_2.11`, `spark-2.4-bigquery`) but are no
longer in the CI matrix.

### Profiles

Because building every connector is lengthy, the project uses
[Maven profiles](https://maven.apache.org/guides/introduction/introduction-to-profiles.html)
to scope the build. The profiles defined in the root POM are:

* `dsv1_2.12`, `dsv1_2.13` — one Scala variant of the DSv1 connector.
* `dsv2_3.1`, `dsv2_3.2`, `dsv2_3.3`, `dsv2_3.4`, `dsv2_3.5`, `dsv2_4.0`,
  `dsv2_4.1` — one Spark version of the DSv2 connector.
* `dsv1`, `dsv2`, `all` — aggregates for the whole DSv1 / DSv2 / both worlds.
* `coverage` — adds the `coverage/` aggregator for Jacoco reports.

Plus, defined in `spark-bigquery-parent/pom.xml`:

* `integration` — enables `*IntegrationTest` classes via Failsafe.
* `acceptance` — enables `*AcceptanceTest` classes via Failsafe.

Example: to compile **just** the Scala 2.12 connector run
`./mvnw install -Pdsv1_2.12`.

**Important:** if no profile is selected, only the always-built common
artifacts are produced.

### JDK requirements

The build targets Java 8 bytecode (`maven.compiler.release=8`), but the test
matrix runs on **two JDKs**:

* **Java 17** — initial install, all unit tests, and integration tests for
  Spark 3.3, 3.4, 3.5, 4.0, and 4.1.
* **Java 8** — integration tests for `dsv1_2.12`, `dsv1_2.13`, `dsv2_3.1`,
  and `dsv2_3.2`.

CI generates `toolchains.xml` automatically via
`./mvnw toolchains:generate-jdk-toolchains-xml`. Locally, install both JDKs
and either generate the toolchains file the same way or set `JAVA_HOME` per
command. See `cloudbuild/presubmit.sh` for the exact mapping of stage to JDK.

### Running tests

Integration and acceptance tests are disabled by default. To run them locally:

```bash
# Unit tests for one connector flavor
./mvnw test -Pcoverage,dsv2_3.5

# Integration tests for one connector flavor
./mvnw failsafe:integration-test failsafe:verify -Pintegration,dsv2_3.5

# Acceptance tests for one connector flavor
./mvnw failsafe:integration-test failsafe:verify -Pacceptance,dsv2_3.5
```

To reproduce the full CI presubmit, follow the sequence in
`cloudbuild/presubmit.sh`. (DSv1 + DSv2 + all integration profiles, with the
correct JDK per stage.)

### GCP setup for integration / acceptance tests

The test-runner GCP user needs permissions to create and delete BigQuery
datasets and tables in the test project, and to read, write, and delete
objects in the test buckets in GCS.

The following environment variables are required to run integration tests:

* `GOOGLE_APPLICATION_CREDENTIALS` — full path to a credentials JSON, either a
  service account or the result of a `gcloud auth login` run.
* `GOOGLE_CLOUD_PROJECT` — Google Cloud project used to test the connector.
* `TEMPORARY_GCS_BUCKET` — GCS bucket for staging writes during integration
  tests.
* `BIGLAKE_CONNECTION_ID` — Cloud Resource connection ID used by BigLake table
  tests.
* `BIGQUERY_KMS_KEY_NAME` — KMS key used by encrypted-table tests.

Acceptance tests additionally require:

* `ACCEPTANCE_TEST_BUCKET` — GCS bucket reserved for acceptance scenarios.
* `SERVERLESS_NETWORK_URI` — VPC network used by the serverless Dataproc
  batches.
