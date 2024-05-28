#!/bin/sh

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
  exit 0
fi
