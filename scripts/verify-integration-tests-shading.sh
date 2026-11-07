#!/bin/sh

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks the runtime integration tests jar (spark-bigquery-integration-tests): apart from the
# relocated dependencies, it may only contain the integration tests, their acceptance helper,
# JUnit and Hamcrest, and none of the connector's own resources.

JAR_FILE="$1"

jar tf "${JAR_FILE}" | \
  grep -E "\.class$" | \
  grep -v '^com/google/cloud/spark/bigquery/repackaged/' | \
  grep -v '^com/google/cloud/spark/bigquery/integration/' | \
  grep -v '^com/google/cloud/spark/bigquery/acceptance/AcceptanceTestUtils' | \
  grep -v '^com/google/cloud/bigquery/connector/common/integration/' | \
  grep -vE '^(junit|org/junit|org/hamcrest)/'
CLASSES_FOUND=$?

jar tf "${JAR_FILE}" | grep -x 'spark-bigquery-connector.properties'
RESOURCES_FOUND=$?

if [ ${CLASSES_FOUND} -eq 0 ] || [ ${RESOURCES_FOUND} -eq 0 ]; then
  echo ""
  echo "Found unexpected unshaded classes or connector resources, please fix above findings"
  exit 1
else
  echo "No unexpected unshaded classes found"
  exit 0
fi
