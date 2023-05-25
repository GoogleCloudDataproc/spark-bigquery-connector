import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BigNumeric acceptance test').getOrCreate()
table = sys.argv[1]

df = spark.read.format("bigquery").load(table)

min = "-0.34992332820282019728792003956564819968"
max = "0.34992332820282019728792003956564819967"

data = df.select("min", "max").collect()

for row in data:
  bigNumMin = row['min']
  bigNumMax = row['max']
  print(str(bigNumMin) == min)
  print(str(bigNumMax) == max)

df.coalesce(1).write.csv(sys.argv[2])
