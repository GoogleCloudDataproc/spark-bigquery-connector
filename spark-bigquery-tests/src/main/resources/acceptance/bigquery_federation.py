#!/usr/bin/env python
# Copyright 2018 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import sys
from pyspark.sql import SparkSession

def random_str():
	return str(time.time()).replace(".", "_")

spark = SparkSession.builder.appName('Metastore acceptance Test').getOrCreate()
spark.conf.set("temporaryGcsBucket", sys.argv[1])

test_dataset = "spark_bigquery_" + random_str()
print(f"Step 0: Creating test dataset {test_dataset}")
datasets = spark.sql("SHOW DATABASES").where(f"namespace='{test_dataset}'").collect()
assert len(datasets) == 0
spark.sql(f"create database {test_dataset}");
datasets = spark.sql("SHOW DATABASES").where(f"namespace='{test_dataset}'").collect()
assert len(datasets) == 1
assert datasets[0].asDict().get('namespace') == test_dataset
print("Dataset has been created")

print("Step 1: Creating test table")
test_table = "simple_" + random_str()
spark.sql(f"create table {test_dataset}.{test_table}(id int, name string)")
spark.sql(f"INSERT INTO {test_dataset}.{test_table} VALUES (1,'Spark'),(2,'Hive')")
spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", test_dataset)
c = spark.read.format("bigquery").load(f"SELECT COUNT(*) FROM {test_dataset}.{test_table}").collect()[0][0]
assert c == 2

#print("Step 2: Creating test table as SELECT")
#df = spark.read.format("bigquery").load("bigquery-public-data.samples.shakespeare")
#df.show()
#df.createTempView("shakespeare")
#src_table = "shakespeare_src_" + random_str()
#dest_table = "shakespeare_dest_" + random_str()
#print(f"writing to {test_dataset}.{src_table}")
# CTAS is not supported yet
#spark.sql("SELECT * FROM shakespeare WHERE word = 'spark'").write.format("bigquery").save(f"{test_dataset}.{src_table}")
#spark.sql(f"CREATE TABLE {test_dataset}.{dest_table} AS SELECT * FROM {test_dataset}.{src_table}")
#c = spark.read.format("bigquery").load(f"SELECT COUNT(*) FROM {test_dataset}.{src_table}").collect()[0][0]
#assert c == 9

print("Step 4: Listing tables")
tables = spark.sql(f"SHOW TABLES IN {test_dataset}")
tables.show()
assert tables.count() == 2 # we have temporary table because of the previous SELECT COUNT(*) query

print("Step 5: Dropping table")
spark.sql(f"DROP TABLE {test_dataset}.{test_table}")
tables2 = spark.sql(f"SHOW TABLES IN {test_dataset}")
tables2.show()
assert tables2.count() == 1

print("Step 6: Dropping the Dataset")
spark.sql(f"DROP DATABASE {test_dataset} CASCADE")
datasets2 = spark.sql("SHOW DATABASES").where(f"namespace='{test_dataset}'")
assert datasets2.count() == 0

print("DONE")
