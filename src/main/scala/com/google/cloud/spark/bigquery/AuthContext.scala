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

import java.io.ByteArrayInputStream

import com.google.api.client.util.Base64
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.sql.SQLContext

case class AuthContext(credentialsBytes: Array[Byte],
                       projectId: String) {

  def toBigQueryCredentials: Credentials =
    GoogleCredentials
      .fromStream(new ByteArrayInputStream(credentialsBytes))

}

object AuthContext {
  def fromSQLContext(sqlContext: SQLContext): Option[AuthContext] =
    for {
      credentials <- getNonEmpty(sqlContext, "bigquery.credentials")
      projectId <- getNonEmpty(sqlContext, "bigquery.projectId")
    } yield AuthContext(Base64.decodeBase64(credentials), projectId)

  private def getNonEmpty(sqlContext: SQLContext, key: String) =
    Option(sqlContext.getConf(key, "")).filter(_.nonEmpty)

}
