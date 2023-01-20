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

readonly STEP=$1

function checkenv() {
  if [ -z "${CODECOV_TOKEN}" ]; then
    echo "missing environment variable CODECOV_TOKEN"
    exit 1
  fi
}

readonly M2REPO="/workspace/.repository"
readonly DATE="$(date +%Y%m%d)"
readonly REVISION="0.0.${DATE}"
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=${M2REPO} -Drevision=${REVISION}"
readonly BUCKET="spark-lib-nightly-snapshots"

mkdir -p ${M2REPO}
cd /workspace

case $STEP in
  build)
    checkenv
    # Build
    $MVN install -DskipTests -Pdsv1,dsv2
    #coverage report
    $MVN test jacoco:report jacoco:report-aggregate -Pcoverage,dsv1,dsv2
    # Run integration tests
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv1,dsv2_2.4,dsv2_3.1,dsv2_3.2,dsv2_3.3
    # Run acceptance tests
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,acceptance,dsv1,dsv2_2.4,dsv2_3.1,dsv2_3.2,dsv2_3.3
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "nightly"

    # Save the REVISION variable to use in the next build step
    echo "${REVISION}" > /workspace/revision.txt
    exit
    ;;

  copy-to-gcs)
    # Get the REVISION variable from the previous step
    readonly BUILD_REVISION="$(cat /workspace/revision.txt)"

    # Upload nightly artifacts to the snapshot bucket and mark nightly snapshot
    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-bigquery-with-dependencies_2.11/${BUILD_REVISION}/spark-bigquery-with-dependencies_2.11-${BUILD_REVISION}.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-bigquery-with-dependencies_2.11-${BUILD_REVISION}.jar" "gs://${BUCKET}/spark-bigquery-with-dependencies_2.11-nightly-snapshot.jar"

    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${BUILD_REVISION}/spark-bigquery-with-dependencies_2.12-${BUILD_REVISION}.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-bigquery-with-dependencies_2.12-${BUILD_REVISION}.jar" "gs://${BUCKET}/spark-bigquery-with-dependencies_2.12-nightly-snapshot.jar"

    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-bigquery-with-dependencies_2.13/${BUILD_REVISION}/spark-bigquery-with-dependencies_2.13-${BUILD_REVISION}.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-bigquery-with-dependencies_2.13-${BUILD_REVISION}.jar" "gs://${BUCKET}/spark-bigquery-with-dependencies_2.13-nightly-snapshot.jar"

    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-2.4-bigquery/${BUILD_REVISION}-preview/spark-2.4-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-2.4-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}/spark-2.4-bigquery-nightly-snapshot-preview.jar"

    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-3.1-bigquery/${BUILD_REVISION}-preview/spark-3.1-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-3.1-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}/spark-3.1-bigquery-nightly-snapshot-preview.jar"

    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-3.2-bigquery/${BUILD_REVISION}-preview/spark-3.2-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-3.2-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}/spark-3.2-bigquery-nightly-snapshot-preview.jar"

    gsutil cp "${M2REPO}/com/google/cloud/spark/spark-3.3-bigquery/${BUILD_REVISION}-preview/spark-3.3-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}"
    gsutil cp "gs://${BUCKET}/spark-3.3-bigquery-${BUILD_REVISION}-preview.jar" "gs://${BUCKET}/spark-3.3-bigquery-nightly-snapshot-preview.jar"

    exit
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac

