set -euxo pipefail

readonly bucket=$1
readonly STEP=$2

echo "start"
gcloud config set project google.com:hadoop-cloud-dev
case $STEP in
  v1-20-bq)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
              --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
              --cluster=spark-sense-c2d-v1-20-bq --region=us-central1 \
              -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
              --bqJarLocation=gs://$bucket/spark-bigquery-with-dependencies_2.12-nightly-snapshot-sparksense.jar \
              --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=1 \
              --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
              --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V1_20_BQ \
              --sparkJobProperties=spark.sql.cbo.joinReorder.enabled=false,spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
              ;;

  v2-20-bq)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=spark-sense-c2d-v2-20-bq --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-3.1-bigquery-nightly-snapshot-preview-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=1 \
          --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V2_20_BQ \
          --sparkJobProperties=spark.sql.cbo.joinReorder.enabled=false,spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
          ;;

  gcs-20)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=spark-sense-c2d-20-gcs --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-bigquery-with-dependencies_2.12-nightly-snapshot-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=1 \
          --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=GCS_20 \
          --sparkJobProperties=spark.sql.cbo.joinReorder.enabled=false,spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
          ;;
  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
