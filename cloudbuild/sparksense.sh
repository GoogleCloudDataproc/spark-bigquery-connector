set -euxo pipefail

readonly bucket=$1
readonly STEP=$2

echo "start"
gcloud config set project google.com:hadoop-cloud-dev
case $STEP in
  v1-20-bq)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
              --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
              --cluster=spark-sense-v1-20-bq --region=us-central1 \
              -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
              --bqJarLocation=gs://$bucket/spark-bigquery-with-dependencies_2.12-nightly-snapshot-sparksense.jar \
              --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
              --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
              --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V1_20_BQ --queriesToRun=q1,q2
              ;;

  v2-20-bq)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=spark-sense-v2-20-bq --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-2.4-bigquery-nightly-snapshot-preview-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
          --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V2_20_BQ --queriesToRun=q1,q2
          ;;

  gcs-20)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=spark-sense-20-gcs --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-bigquery-with-dependencies_2.12-nightly-snapshot-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
          --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=GCS_20 --queriesToRun=q1,q2
          ;;
  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
echo "done"
