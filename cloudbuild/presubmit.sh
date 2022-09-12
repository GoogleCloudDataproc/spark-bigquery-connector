
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

function checkenv() {
  if [ -z "${CODECOV_TOKEN}" ]; then
    echo "missing environment variable CODECOV_TOKEN"
    exit 1
  fi
}

readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    checkenv
    $MVN install -DskipTests -Pdsv1_2.12,dsv1_2.13,dsv2
    exit
    ;;

  # Run unit tests
  unittest)
    $MVN test jacoco:report jacoco:report-aggregate -Pcoverage,dsv1_2.12,dsv1_2.13,dsv2
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${STEP}"
    ;;

  # Run integration tests
  integrationtest-2.12)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv1_2.12
    ;;

  # Run integration tests
  integrationtest-2.13)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv1_2.13
    ;;

  # Run integration tests
  integrationtest-2.4)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_2.4
    ;;

  # Run integration tests
  integrationtest-3.1)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_3.1
    ;;

  upload-it-to-codecov)
    checkenv
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F integrationtest
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
