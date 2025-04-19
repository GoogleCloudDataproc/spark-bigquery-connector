
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

unset MAVEN_OPTS
readonly BUILD_OPTS='-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false'
readonly MVN_NT="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly MVN="${MVN_NT} -t toolchains.xml"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    checkenv
    $MVN_NT toolchains:generate-jdk-toolchains-xml -Dtoolchain.file=toolchains.xml
    cat toolchains.xml

    export MAVEN_OPTS=${BUILD_OPTS}
    export JAVA_HOME=${JAVA8_HOME}
    $MVN -T 1C install -DskipTests -Pdsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2
    export JAVA_HOME=${JAVA17_HOME}
    $MVN -T 1C install -DskipTests -Dscala.skipTests=true -Pdsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0
    exit
    ;;

  # Run unit tests
  unittest)
    export MAVEN_OPTS=${BUILD_OPTS}
    export JAVA_HOME=${JAVA8_HOME}
    $MVN -T 1C test jacoco:report jacoco:report-aggregate -Pcoverage,dsv1_2.12,dsv1_2.13,dsv2_3.1,dsv2_3.2
    export JAVA_HOME=${JAVA17_HOME}
    $MVN -T 1C test jacoco:report jacoco:report-aggregate -Pcoverage,dsv2_3.3,dsv2_3.4,dsv2_3.5,dsv2_4.0 -Dscala.skipTests=true
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${STEP}"
    ;;

  # Run integration tests
  integrationtest-2.12)
    export JAVA_HOME=${JAVA8_HOME}
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv1_2.12
    ;;

  # Run integration tests
  integrationtest-2.13)
    export JAVA_HOME=${JAVA8_HOME}
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv1_2.13
    ;;

  # Run integration tests
  integrationtest-3.1)
    export JAVA_HOME=${JAVA8_HOME}
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_3.1
    ;;

  # Run integration tests
  integrationtest-3.2)
    export JAVA_HOME=${JAVA8_HOME}
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_3.2
    ;;

  # Run integration tests
  integrationtest-3.3)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_3.3
    ;;

  # Run integration tests
  integrationtest-3.4)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_3.4
    ;;

  # Run integration tests
  integrationtest-3.5)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_3.5
    ;;

  # Run integration tests
  integrationtest-4.0)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate -Pcoverage,integration,dsv2_4.0
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
