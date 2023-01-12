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
              --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
              --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
              --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V1_20_BQ \
              --queriesToRun=q1,q10,q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q2,q20,q21,q22,q23a,q23b,q25,q26,q27,q28,q29,q3,q30,q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q4,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q5,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q6,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q7,q70,q71,q73,q74,q75,q76,q77,q78,q79,q8,q80,q81,q82,q83,q84,q85,q86,q87,q88,q89,q9,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99 \
              --sparkJobProperties=spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
              ;;

  v2-20-bq)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=spark-sense-c2d-v2-20-bq --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-3.1-bigquery-nightly-snapshot-preview-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
          --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=V2_20_BQ \
          --queriesToRun=q1,q10,q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q2,q20,q21,q22,q23a,q23b,q25,q26,q27,q28,q29,q3,q30,q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q4,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q5,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q6,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q7,q70,q71,q73,q74,q75,q76,q77,q78,q79,q8,q80,q81,q82,q83,q84,q85,q86,q87,q88,q89,q9,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99 \
          --sparkJobProperties=spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
          ;;

  gcs-20)
    gcloud dataproc jobs submit spark --project=google.com:hadoop-cloud-dev --class=com.google.cloud.performance.BenchmarkRunner \
          --jars=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --cluster=spark-sense-c2d-20-gcs --region=us-central1 \
          -- --benchmark=tpcds --jarLocation=gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar \
          --bqJarLocation=gs://$bucket/spark-bigquery-with-dependencies_2.12-nightly-snapshot-sparksense.jar \
          --resultLocation=gs://suryasoma-public/sparksense/tpcds/bq --iterations=3 \
          --bqTablePath=spark_sense_tpcds_bigquery_runID  --tempBucket=suryasoma-public \
          --runType=bq --database=tpcds_1T_partitioned_gcs --runID=GCS_20 \
          --queriesToRun=q1,q10,q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q2,q20,q21,q22,q23a,q23b,q25,q26,q27,q28,q29,q3,q30,q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q4,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q5,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q6,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q7,q70,q71,q73,q74,q75,q76,q77,q78,q79,q8,q80,q81,q82,q83,q84,q85,q86,q87,q88,q89,q9,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99 \
          --sparkJobProperties=spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
          ;;
  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
