import sys
from pyspark.sql import SparkSession

try:
    import pkg_resources

    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)

spark = SparkSession.builder.appName('BigNumeric acceptance test').getOrCreate()
table = sys.argv[1]

df = spark.read.format("bigquery").load(table)

min = "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
max = "578960446186580977117854925043439539266.34992332820282019728792003956564819967"

data = df.select("min", "max").collect()

for row in data:
  bigNumMin = row['min']
  bigNumMax = row['max']
  print(str(bigNumMin.number) == min)
  print(str(bigNumMax.number) == max)

df.coalesce(1).write.csv(sys.argv[2])
