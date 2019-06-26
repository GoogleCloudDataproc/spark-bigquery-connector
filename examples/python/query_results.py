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
from google.cloud import bigquery
from pyspark.sql import SparkSession

# Currently this only supports queries which have at least 10 MB of results
QUERY = """
SELECT *
FROM `bigquery-public-data.san_francisco.bikeshare_stations` s
JOIN `bigquery-public-data.san_francisco.bikeshare_trips` t
ON s.station_id = t.start_station_id
"""

spark = SparkSession.builder.appName('Query Results').getOrCreate()
bq = bigquery.Client()

print('Querying BigQuery')
query_job = bq.query(QUERY)

# Wait for query execution
query_job.result()

df = spark.read.format('bigquery') \
    .option('dataset', query_job.destination.dataset_id) \
    .option('table', query_job.destination.table_id) \
    .load()

print('Reading query results into Spark')
df.show()

