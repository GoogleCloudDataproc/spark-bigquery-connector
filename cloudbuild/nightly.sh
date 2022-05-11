#!/bin/bash

# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail

if [ -z "${CODECOV_TOKEN}" ]; then
  echo "missing environment variable CODECOV_TOKEN"
  exit 1
fi

readonly DATE="$(date +%Y%m%d)"
readonly REVISION="0.0.${DATE}"
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository -Drevision=${REVISION}"

cd /workspace

# Build
$MVN install -DskipTests -Pdsv1_2.12,dsv2
#coverage report
$MVN test jacoco:report jacoco:report-aggregate -Pcoverage,dsv1_2.12,dsv2
# Run integration tests
$MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv1_2.12,dsv2_2.4
# Run acceptance tests
$MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,acceptance,dsv1_2.12,dsv2_2.4
# Upload test coverage report to Codecov
bash <(curl -s https://codecov.io/bash) -K -F "nightly"

# Upload daily artifacts to the snapshot bucket
gsutil cp \
  "spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.11/target/spark-bigquery-with-dependencies_2.11-${REVISION}.jar" \
  "spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.12/target/spark-bigquery-with-dependencies_2.12-${REVISION}.jar" \
  "spark-bigquery-dsv2/spark-2.4-bigquery/target/spark-2.4-bigquery-${REVISION}-preview.jar" \
  "gs://${BUCKET}/"
# Marking daily snapshot
gsutil cp \
  "gs://${BUCKET}/spark-bigquery-with-dependencies_2.11-${REVISION}.jar" \
  "gs://${BUCKET}/spark-bigquery-with-dependencies_2.11-daily-snapshot.jar"
gsutil cp \
  "gs://${BUCKET}/spark-bigquery-with-dependencies_2.12-${REVISION}.jar" \
  "gs://${BUCKET}/spark-bigquery-with-dependencies_2.12-daily-snapshot.jar"
gsutil cp \
  "gs://${BUCKET}/spark-2.4-bigquery-${REVISION}-preview.jar" \
  "gs://${BUCKET}/spark-2.4-bigquery-daily-snapshot-preview.jar"


