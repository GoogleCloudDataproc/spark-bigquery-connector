/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.examples

import java.nio.file.Files

import com.google.cloud.spark.bigquery._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Shakespeare extends Logging {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("test").getOrCreate()
    val sc = spark.sparkContext

    var df = spark.read.bigquery("publicdata.samples.shakespeare").cache()
    df.show()
    df.printSchema()
    val path = Files.createTempDirectory("spark-bigquery").resolve("out")
    log.warn("Writing table out to {}", path)
    df.write.csv(path.toString)
  }
}
