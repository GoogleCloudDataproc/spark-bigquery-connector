#!/bin/sh

# Copyright 2024 Google LLC
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

JAR_FILE="$1"

jar tvf "${JAR_FILE}" | \
  grep -v META-INF | \
  grep -E "\.class$" | \
  grep -v com/google/cloud/spark/bigquery | \
  grep -v com/google/cloud/bigquery/connector/common | \
  grep -vE 'org/apache/spark/sql/.*SparkSqlUtils.class'

if [ $? -eq 0 ]; then
  # grep found classes where there shouldn't be any. Print error message and exit
  echo ""
  echo "Found unshaded classes, please fix above findings"
  exit 1
else
  echo "No unshaded classes found"
fi

# Check that every relocated class referenced by the classes in the jar is in the jar. A
# dependency that the shade plugin leaves out (for example because it inherits the provided
# scope of Spark's own dependency on it) is otherwise only noticed at runtime, as a
# NoClassDefFoundError in the classes that reference it.

RELOCATION_PREFIX="com.google.cloud.spark.bigquery.repackaged."

# Relocated classes that the shaded libraries reference but that are deliberately not packaged,
# as extended regular expressions matched against the class name without the relocation prefix.
ALLOWED_MISSING_CLASSES='
# The App Engine transport and data store of the Google HTTP client, used only on App Engine.
^com\.google\.appengine\.
# The v1beta1 and v1beta2 BigQuery Storage APIs, excluded since the connector uses v1.
^com\.google\.cloud\.bigquery\.storage\.v1beta[12]\.
# grpc-netty-shaded, excluded in favor of grpc-netty. Referenced by grpc-alts.
^io\.grpc\.netty\.shaded\.
# The xDS support used by the google-c2p name resolver of grpc-googleapis, not packaged.
^io\.grpc\.xds\.
# Optional test certificate generator of netty-handler.
^io\.netty\.pkitesting\.
# OpenLineage client transports and metrics, which spark-extension-interfaces does not include.
^io\.openlineage\.spark\.shade\.client\.(circuitBreaker|metrics|transports)\.
# The Beam SDK core, excluded from beam-sdks-java-io-hadoop-common. Used by its Beam coders.
^org\.apache\.beam\.sdk\.(annotations|coders|values)\.
# XZ and LZMA support, an optional dependency of commons-compress.
^org\.tukaani\.xz\.
'
ALLOWED_PATTERN=$(printf '%s\n' "${ALLOWED_MISSING_CLASSES}" | grep -v -e '^#' -e '^$' | paste -sd '|' -)

# jdeps lists each class dependency as "<class> -> <dependency> <location>", with "not found" as
# the location when the dependency is neither in the jar nor in the JDK.
DEPENDENCIES=$(jdeps -verbose:class "${JAR_FILE}")
if [ $? -ne 0 ]; then
  echo "jdeps failed on ${JAR_FILE}"
  exit 1
fi

MISSING_CLASSES=$(printf '%s\n' "${DEPENDENCIES}" | \
  awk -v prefix="${RELOCATION_PREFIX}" '
    $2 == "->" && $(NF-1) == "not" && $NF == "found" && index($3, prefix) == 1 {
      missing = substr($3, length(prefix) + 1)
      if (!(missing in referrers)) {
        referrers[missing] = substr($1, index($1, prefix) == 1 ? length(prefix) + 1 : 1)
      }
    }
    END {
      for (missing in referrers) {
        print missing " (referenced by " referrers[missing] ")"
      }
    }' | \
  grep -v -E "${ALLOWED_PATTERN}" | \
  LC_ALL=C sort)

if [ -n "${MISSING_CLASSES}" ]; then
  printf '%s\n' "${MISSING_CLASSES}"
  echo ""
  echo "Found references to relocated classes (without the ${RELOCATION_PREFIX} prefix) that"
  echo "the jar doesn't contain, please fix above findings"
  exit 1
else
  echo "No missing relocated classes found"
  exit 0
fi
