set -euxo pipefail

readonly BUCKET=$1
readonly RUN_ID=$2
readonly CLUSTER=$3
readonly REGION=$4
readonly BENCHMARK=$5
readonly BQ_JAR=$6
readonly RUN_TYPE=$7
readonly DATABASE=$8

readonly PROJECT="google.com:hadoop-cloud-dev"
readonly SPARK_BENCHMARK_KIT_JAR="gs://suryasoma-public/sparksense/jars/runID/spark-benchmark-kit-1.0-SNAPSHOT-jar-with-dependencies.jar"
readonly CLASS="com.google.cloud.performance.BenchmarkRunner"
#readonly QUERIES_TO_RUN="q1,q10,q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q2,q20,q21,q22,q23a,q23b,q25,q26,q27,q28,q29,q3,q30,q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q4,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q5,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q6,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q7,q70,q71,q73,q74,q75,q76,q77,q78,q79,q8,q80,q81,q82,q83,q84,q85,q86,q87,q88,q89,q9,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99"
readonly QUERIES_TO_RUN="q1,q10,q11"
readonly RESULT_LOCATION="gs://suryasoma-public/sparksense/tpcds/bq"
readonly NUM_OF_ITERATIONS=2
readonly BQ_TABLE_PATH="surya_spark_sense_test" #TODO: rename to spark_sense_tpcds_bigquery_runID
readonly TEMP_BUCKET="suryasoma-public"
readonly SPARK_JOB_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.executor.instances=9999,spark.dataproc.sql.optimizer.intersect.optimization.enabled=true,spark.dataproc.sql.local.rank.pushdown.enabled=true,spark.dataproc.sql.optimizer.leftsemijoin.conversion.enabled=true,spark.dataproc.sql.parquet.enableFooterCache=true,spark.dataproc.sql.joinConditionReorder.enabled=true,spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false"

echo "start"
echo $BUCKET
echo $RUN_ID
echo $CLUSTER
echo $REGION
echo $BENCHMARK
echo $BQ_JAR
echo $RUN_TYPE
echo $DATABASE

echo $PROJECT
echo $SPARK_BENCHMARK_KIT_JAR
echo $CLASS
echo $QUERIES_TO_RUN
echo $RESULT_LOCATION
echo $NUM_OF_ITERATIONS
echo $BQ_TABLE_PATH
echo $TEMP_BUCKET
echo $SPARK_JOB_PROPERTIES

gcloud config set project $PROJECT
gcloud dataproc jobs submit spark --project=$PROJECT \
    --class=$CLASS \
    --jars=$SPARK_BENCHMARK_KIT_JAR \
    --cluster=$CLUSTER \
    --region=$REGION \
    -- --benchmark=$BENCHMARK \
    --jarLocation=$SPARK_BENCHMARK_KIT_JAR \
    --bqJarLocation=gs://$BUCKET/$BQ_JAR \
    --resultLocation=$RESULT_LOCATION \
    --iterations=$NUM_OF_ITERATIONS \
    --bqTablePath=$BQ_TABLE_PATH \
    --tempBucket=$TEMP_BUCKET \
    --runType=$RUN_TYPE \
    --database=$DATABASE \
    --runID=$RUN_ID \
    --queriesToRun=$QUERIES_TO_RUN \
    --sparkJobProperties=$SPARK_JOB_PROPERTIES
echo "done"

