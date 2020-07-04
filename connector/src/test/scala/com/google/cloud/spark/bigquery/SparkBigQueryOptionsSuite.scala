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

import com.google.cloud.bigquery.JobInfo
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class SparkBigQueryOptionsSuite extends FunSuite {

  val hadoopConfiguration = new Configuration
  hadoopConfiguration.set(SparkBigQueryOptions.GcsConfigCredentialsFileProperty, "hadoop_cfile")
  hadoopConfiguration.set(SparkBigQueryOptions.GcsConfigProjectIdProperty, "hadoop_project")

  val parameters = Map("table" -> "dataset.table")
  val sparkVersion = "2.4.0"

  test("taking credentials file from GCS hadoop config") {
    assertResult(Some("hadoop_cfile")) {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        hadoopConfiguration,
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
        None) // schema
      options.credentialsFile
    }
  }

  test("no credentials file is provided") {
    val options = SparkBigQueryOptions(
      parameters,
      Map.empty[String, String], // allConf
      new Configuration,
      new SQLConf,
      sparkVersion,
      None) // schema
    assert(options.credentialsFile.isEmpty)
  }

  test("taking project id from GCS hadoop config") {
    assertResult("hadoop_project") {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        hadoopConfiguration,
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
          new SQLConf,
          sparkVersion,
          None) // schema
      }
      assert (thrown.getMessage ==
        "Data read format 'ABC' is not supported. Supported formats are 'ARROW,AVRO'")
  }

  test("data format - no value set") {
    assertResult("ARROW") {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.readDataFormat.toString
    }
  }

  test("Set Read Data Format as Avro") {
    assertResult("AVRO") {
      val options = SparkBigQueryOptions(
        parameters + ("readDataFormat" -> "Avro"),
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
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
        new SQLConf,
        sparkVersion,
        None) // schema
      options.maxParallelism
    }
  }
  
  test("loadSchemaUpdateOption - allowFieldAddition") {
    assertResult(Seq(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION)) {
      val options = SparkBigQueryOptions(
        parameters + ("allowFieldAddition" -> "true"),
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.loadSchemaUpdateOptions.asScala.toSeq
    }
  }

  test("loadSchemaUpdateOption - allowFieldRelaxation") {
    assertResult(Seq(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)) {
      val options = SparkBigQueryOptions(
        parameters + ("allowFieldRelaxation" -> "true"),
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.loadSchemaUpdateOptions.asScala.toSeq
    }
  }

  test("loadSchemaUpdateOption - both") {
    assertResult(Seq(
      JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
      JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)) {
      val options = SparkBigQueryOptions(
        parameters + ("allowFieldAddition" -> "true", "allowFieldRelaxation" -> "true"),
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.loadSchemaUpdateOptions.asScala.toSeq
    }
  }

  test("loadSchemaUpdateOption - none") {
    assertResult(true) {
      val options = SparkBigQueryOptions(
        parameters,
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.loadSchemaUpdateOptions.isEmpty
    }
  }

  test("normalize All Conf") {
    val originalConf = Map("key1" -> "val1", "spark.datasource.bigquery.key2" -> "val2")
    val normalizedConf = SparkBigQueryOptions.normalizeAllConf(originalConf)

    assert(normalizedConf.get("key1")  == Some("val1"))
    assert(normalizedConf.get("key2")  == Some("val2"))
  }

  test("Set persistentGcsPath") {
    assertResult(Some("/persistent/path")) {
      val options = SparkBigQueryOptions(
        parameters + ("persistentGcsPath" -> "/persistent/path"),
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.persistentGcsPath
    }
  }

  test("Set persistentGcsBucket") {
    assertResult(Some("gs://persistentGcsBucket")) {
      val options = SparkBigQueryOptions(
        parameters + ("persistentGcsBucket" -> "gs://persistentGcsBucket"),
        Map.empty[String, String], // allConf
        new Configuration,
        new SQLConf,
        sparkVersion,
        None) // schema
      options.persistentGcsBucket
    }
  }
}
