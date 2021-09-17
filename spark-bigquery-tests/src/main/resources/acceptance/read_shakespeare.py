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

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Shakespeare on Spark').getOrCreate()

table = 'bigquery-public-data.samples.shakespeare'
df = spark.read.format('bigquery').load(table)
# Only these columns will be read
df = df.select('word', 'word_count')
# The filters that are allowed will be automatically pushed down.
# Those that are not will be computed client side
df = df.where("word_count > 0 AND word='spark'")
# Further processing is done inside Spark
df = df.groupBy('word').sum('word_count')

print('The resulting schema is')
df.printSchema()

print('Spark mentions in Shakespeare')
df.show()

df.coalesce(1).write.csv(sys.argv[1])