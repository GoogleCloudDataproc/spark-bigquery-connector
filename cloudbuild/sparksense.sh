set -euxo pipefail

readonly bucket=$1

echo "start"
gcloud config set project google.com:hadoop-cloud-dev
gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=sparksense-v1-bq-3 --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-bigquery-with-dependencies_2.12-nightly-snapshot-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
          --bqTablePath=surya_spark_sense_test  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V1_20_BQ --queriesToRun=q1,q2
echo "done"
