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

import java.util.{Optional, Properties}

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Schema}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
 * Static helpers for working with BigQuery, relevant only to the Scala code
 */
object BigQueryUtilScala extends Logging{
  private val PARSER = new JacksonFactory()

  def noneIfEmpty(s: String): Option[String] = Option(s).filterNot(_.trim.isEmpty)

  // validating that the connector's scala version and the runtime's scala
  // version are the same
  def validateScalaVersionCompatibility(): Unit = {
    val runtimeScalaVersion = trimVersion(scala.util.Properties.versionNumberString)
    val buildProperties = new Properties
    buildProperties.load(getClass.getResourceAsStream("/spark-bigquery-connector.properties"))
    val connectorScalaVersion = trimVersion(buildProperties.getProperty("scala.version"))
    if (!runtimeScalaVersion.equals(connectorScalaVersion)) {
      throw new IllegalStateException(
        s"""
           |This connector was made for Scala $connectorScalaVersion,
           |it was not meant to run on Scala $runtimeScalaVersion"""
          .stripMargin.replace('\n', ' '))
    }
  }

  private def trimVersion(version: String) =
    version.substring(0, version.lastIndexOf('.'))

  def toSeq[T](list: java.util.List[T]): Seq[T] = list.asScala.toSeq

  def toJavaIterator[T](it: Iterator[T]): java.util.Iterator[T] = it.asJava


  def createBigQuery(options: SparkBigQueryConfig): BigQuery = {
    val credentials = options.createCredentials()
    val parentProjectId = options.getParentProjectId()
    logInfo(
      s"BigQuery client project id is [$parentProjectId}], derived from the parentProject option")
    BigQueryOptions
      .newBuilder()
      .setProjectId(parentProjectId)
      .setCredentials(credentials)
      .build()
      .getService
  }

  private def dtoTableSchemaToBqSchema(dtoSchema: TableSchema): Schema = {
    val fromPbMethod =
      classOf[Schema]
        .getDeclaredMethods
        .toIterable
        .find(method => method.getName == "fromPb")
        .get

    fromPbMethod.setAccessible(true)
    fromPbMethod.invoke(null, dtoSchema).asInstanceOf[Schema]
  }

  def parseSchemaFromJson(jsonSchemaString: String): Option[Schema] = {
    try {
      val tableFieldSchema = PARSER
        .createJsonParser(jsonSchemaString)
        .parse(classOf[TableFieldSchema])

      if (tableFieldSchema.getFields.isEmpty) {
        None
      }

      val schemaDto = new TableSchema()
        .setFields(tableFieldSchema.getFields)

      Some(dtoTableSchemaToBqSchema(schemaDto))
    } catch {
      case e: Exception =>
        logError(s"BQ json schema couldn't be parsed: $jsonSchemaString", e)
        None
    }
  }


  def toOption[T](javaOptional: Optional[T]): Option[T] =
    if (javaOptional.isPresent) Some(javaOptional.get) else None

  def toOptional[T](option: Option[T]): Optional[T] =
    if (option.isDefined) Optional.of(option.get) else Optional.empty()
}
