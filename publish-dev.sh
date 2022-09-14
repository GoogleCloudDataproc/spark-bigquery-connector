#!/bin/bash
set -e

cd `dirname $0`

DEPLOY_PATH=$1

if [[ -z $DEPLOY_PATH ]]; then
    echo "Error: Must pass location of google storage destination dir"
    echo "Usage: 'publish.sh <gs location>' Note: location doesn't need to include 'gs://' ie 'dev-\$USER/dataproc'"
    echo "Example: 'publish.sh okera-internal-release/latest-$(whoami)/client'"
    exit 1
fi

mvn spotless:apply
./mvnw clean install -P dsv1

BIGQUERY_CONNECTOR_JAR_2_11="spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.11/target/spark-bigquery-with-dependencies_2.11-okera-0.0.1-SNAPSHOT.jar"
BIGQUERY_CONNECTOR_JAR_2_12="spark-bigquery-dsv1/spark-bigquery-with-dependencies_2.12/target/spark-bigquery-with-dependencies_2.12-okera-0.0.1-SNAPSHOT.jar"

gsutil cp $BIGQUERY_CONNECTOR_JAR_2_11 "gs://$DEPLOY_PATH/spark-bigquery-with-dependencies_2.11-okera.jar"
gsutil cp $BIGQUERY_CONNECTOR_JAR_2_12 "gs://$DEPLOY_PATH/spark-bigquery-with-dependencies_2.12-okera.jar"