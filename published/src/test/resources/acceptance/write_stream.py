import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName('Write Stream Test').getOrCreate()

json_location = sys.argv[1]
table_name = sys.argv[2]
temporary_gcs_bucket = sys.argv[3]

schema = StructType().add("col1", "integer").add("col2", "string")

# creating read stream from json(s)
streamingDF = spark.readStream.option("multiline","true").schema(schema).json(json_location)

# writing to bigquery via write stream
stream_writer = streamingDF.writeStream.format("bigquery") \
    .option("checkpointLocation", "/tmp/" + table_name + "/") \
    .outputMode("append") \
    .option("table", table_name) \
    .option("temporaryGcsBucket", temporary_gcs_bucket) \
    .start()

# waiting for 60 seconds for the write to finish
stream_writer.awaitTermination(60)