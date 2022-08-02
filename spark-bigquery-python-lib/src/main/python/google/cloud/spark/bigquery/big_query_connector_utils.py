def enablePushdownSession(spark):
  spark.sparkContext._jvm.com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorPushdown.enablePushdownSession(spark._jsparkSession)


def disablePushdownSession(spark):
  spark.sparkContext._jvm.com.google.cloud.spark.bigquery.pushdowns.BigQueryConnectorPushdown.disablePushdownSession(spark._jsparkSession)
