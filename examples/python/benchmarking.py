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

# An example that shows how to query BigQuery and read those results into Spark.
#
# It relies on https://pypi.org/project/google-cloud-bigquery. To see how to
# install python libraries into Google Cloud Dataproc see
# https://cloud.google.com/dataproc/docs/tutorials/python-configuration

from __future__ import print_function
import time
import getpass
import sys
import csv
import imp
from pyspark.sql import SparkSession

table = sys.argv[1]
optable = sys.argv[2]
spark = SparkSession.builder.appName('Benchmarking').config('spark.jars.packages','com.databricks:spark-avro_2.11:4.0.0').getOrCreate()

bucket = "gaurangisaxena"
spark.conf.set("temporaryGcsBucket", bucket)

df = spark.read.format('bigquery').option('table', table).option('readDataFormat', 'avro').load().cache()
df.write.csv("local2.csv")

for x in range(0, 5):
    outputTable = "benchmark.experiment" + str(x) + "output" + optable
    print('output table',outputTable)
    start = time.time()
    df.write.format("bigquery").mode("overwrite").option("table", outputTable + "orc").option("intermediateFormat", "orc").save()
    orcTime = time.time() - start

    start = time.time()
    df.write.format("bigquery").mode("overwrite").option("table", outputTable + "parquet").option("intermediateFormat", "parquet").save()
    parquetTime = time.time() - start

    start = time.time()
    df.write.format("bigquery").mode("overwrite").option("table", outputTable + "avro").option("intermediateFormat", "avro").save()
    avroTime = time.time() - start

    print('orc','parquet','avro')
    print(orcTime,parquetTime,avroTime)


