def enablePushdownSession(spark):
  spark.sparkContext._jvm.com.google.cloud.spark.bigquery.BigQueryConnectorUtils.enablePushdownSession(spark._jsparkSession)


def disablePushdownSession(spark):
  spark.sparkContext._jvm.com.google.cloud.spark.bigquery.BigQueryConnectorUtils.disablePushdownSession(spark._jsparkSession)
