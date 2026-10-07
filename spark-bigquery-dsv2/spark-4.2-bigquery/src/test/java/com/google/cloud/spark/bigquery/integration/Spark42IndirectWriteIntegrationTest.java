/*
 * Copyright 2026 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Before;

public class Spark42IndirectWriteIntegrationTest extends WriteIntegrationTestBase {

  public Spark42IndirectWriteIntegrationTest() {
    super(SparkBigQueryConfig.WriteMethod.INDIRECT, DataTypes.TimestampNTZType);
  }

  @Before
  public void setParquetLoadBehaviour() {
    // TODO: make this the default value
    spark.conf().set("enableListInference", "true");

    // Spark 4.2 upgrades Apache Hadoop to 3.5.0, whose core-default.xml sets fs.gs.impl to
    // org.apache.hadoop.fs.gs.GoogleHadoopFileSystem and uses byte-unit suffixes (e.g. "64m")
    // that are incompatible with the gcs-connector test dependency. Additionally, Spark 4.2 calls
    // committer.setupJob(job) in FileFormatWriter before setting spark.sql.sources.writeJobUUID,
    // which causes Hadoop's default ManifestCommitter for gs:// to fail with a NullPointerException
    // on job.getJobID() when writing Avro intermediate files.
    Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    hadoopConf.set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    hadoopConf.set("fs.gs.block.size", "67108864");
    hadoopConf.set("fs.gs.outputstream.buffer.size", "8388608");
    hadoopConf.set("fs.gs.inputstream.inplace.seek.limit", "8388608");
    hadoopConf.set("fs.gs.inputstream.min.range.request.size", "2097152");
    hadoopConf.set(
        "mapreduce.outputcommitter.factory.scheme.gs",
        "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory");
  }

  // tests from superclass
}
