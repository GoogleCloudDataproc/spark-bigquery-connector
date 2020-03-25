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
package com.google.cloud.spark.bigquery

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

class SparkBigQueryOptionsSuite extends FunSuite {

  val hadoopConfiguration = new Configuration
  hadoopConfiguration.set(SparkBigQueryOptions.GcsConfigCredentialsFileProperty, "hadoop_cfile")
  hadoopConfiguration.set(SparkBigQueryOptions.GcsConfigProjectIdProperty, "hadoop_project")

  val parameters = Map("table" -> "dataset.table")

  test("taking credentials file from GCS hadoop config") {
    assertResult(Some("hadoop_cfile")) {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        hadoopConfiguration,
        None) // schema
      options.credentialsFile
    }
  }

  test("taking credentials file from the properties") {
    assertResult(Some("cfile")) {
      val options = SparkBigQueryOptions(
        parameters + ("credentialsFile" -> "cfile"),
        Map.empty[String, String], // allConf
        hadoopConfiguration,
        None) // schema
      options.credentialsFile
    }
  }

  test("no credentials file is provided") {
    val options = SparkBigQueryOptions(
      parameters,
      Map.empty[String, String], // allConf
      new Configuration,
      None) // schema
    assert(options.credentialsFile.isEmpty)
  }

  test("taking project id from GCS hadoop config") {
    assertResult("hadoop_project") {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        hadoopConfiguration,
        None) // schema
      options.tableId.getProject
    }
  }

  test("taking project id from the properties") {
    assertResult("pid") {
      val options = SparkBigQueryOptions(
        parameters + ("project" -> "pid"),
        Map.empty[String, String], // allConf
        hadoopConfiguration,
        None) // schema
      options.tableId.getProject
    }
  }

  test("no project id is provided") {
    assertResult(null) {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.tableId.getProject
    }
  }

  test("Invalid data format") {
      val thrown = intercept[Exception] {
        SparkBigQueryOptions(
          parameters + ("readDataFormat" -> "abc"),
          Map.empty[String, String], // allConf
          new Configuration,
          None) // schema
      }
      assert (thrown.getMessage contains
        "Data read format 'ABC' is not supported. Supported formats are Set(ARROW, AVRO)")
  }

  test("data format - no value set") {
    assertResult("AVRO") {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.readDataFormat.toString
    }
  }

  test("Set Read Data Format as Arrow") {
    assertResult("ARROW") {
      val options = SparkBigQueryOptions(
        parameters + ("readDataFormat" -> "Arrow"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.readDataFormat.toString
    }
  }

  test("getAnyOptionWithFallback - only new config exist") {
    assertResult(Some("foo")) {
      val options = SparkBigQueryOptions(
        parameters + ("materializationProject" -> "foo"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.materializationProject
    }
  }

  test("getAnyOptionWithFallback - both configs exist") {
    assertResult(Some("foo")) {
      val options = SparkBigQueryOptions(
        parameters + ("materializationProject" -> "foo", "viewMaterializationProject" -> "bar"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.materializationProject
    }
  }

  test("getAnyOptionWithFallback - only old config exist") {
    assertResult(Some("bar")) {
      val options = SparkBigQueryOptions(
        parameters + ("viewMaterializationProject" -> "bar"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.materializationProject
    }
  }

  test("getAnyOptionWithFallback - no config exist") {
    assertResult(None) {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.materializationProject
    }
  }

  test("maxParallelism - only new config exist") {
    assertResult(Some(3)) {
      val options = SparkBigQueryOptions(
        parameters + ("maxParallelism" -> "3"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.maxParallelism
    }
  }

  test("maxParallelism - both configs exist") {
    assertResult(Some(3)) {
      val options = SparkBigQueryOptions(
        parameters + ("maxParallelism" -> "3", "parallelism" -> "10"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.maxParallelism
    }
  }

  test("maxParallelism - only old config exist") {
    assertResult(Some(10)) {
      val options = SparkBigQueryOptions(
        parameters + ("parallelism" -> "10"),
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.maxParallelism
    }
  }

  test("maxParallelism - no config exist") {
    assertResult(None) {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        new Configuration,
        None) // schema
      options.maxParallelism
    }
  }
}
