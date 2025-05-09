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
package com.google.cloud.spark.bigquery;

import static java.lang.String.format;

import com.google.cloud.bigquery.connector.common.GcpUtil;
import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import java.util.Optional;
import scala.util.Properties;

/** Provides the versions of the client environment in an anonymous way. */
public class SparkBigQueryConnectorUserAgentProvider implements UserAgentProvider {

  // getGcpZone() and not getGcpRegion for backwards compatibility
  @VisibleForTesting
  static String GCP_REGION_PART =
      GcpUtil.getGcpZone().map(region -> " region/" + region).orElse("");

  @VisibleForTesting
  static String DATAPROC_IMAGE_PART =
      Optional.ofNullable(System.getenv("DATAPROC_IMAGE_VERSION"))
          .map(image -> " dataproc-image/" + image)
          .orElse("");

  // In order to avoid using SparkContext or SparkSession, we are going directly to the source
  private static String SPARK_VERSION = org.apache.spark.package$.MODULE$.SPARK_VERSION();
  private static String JAVA_VERSION = System.getProperty("java.runtime.version");
  private static String SCALA_VERSION = Properties.versionNumberString();
  static final String USER_AGENT =
      format(
          "spark/%s java/%s scala/%s%s%s",
          SPARK_VERSION, JAVA_VERSION, SCALA_VERSION, GCP_REGION_PART, DATAPROC_IMAGE_PART);

  private String dataSourceVersion;
  private Optional<String> gpn;

  public SparkBigQueryConnectorUserAgentProvider(String dataSourceVersion, Optional<String> gpn) {
    this.dataSourceVersion = dataSourceVersion;
    this.gpn = gpn;
  }

  @Override
  public String getUserAgent() {

    StringBuilder userAgentBuilder = new StringBuilder();
    userAgentBuilder
        .append("spark-bigquery-connector/")
        .append(SparkBigQueryUtil.CONNECTOR_VERSION);
    gpn.ifPresent(s -> userAgentBuilder.append(" (GPN:").append(s).append(") "));
    userAgentBuilder.append(USER_AGENT).append(" datasource/").append(dataSourceVersion);

    return userAgentBuilder.toString();
  }

  @Override
  /*
   * Creates JsonObject of the conenctor info as Json format is used at the receiver of this event.
   */
  public String getConnectorInfo() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("connectorVersion", SparkBigQueryUtil.CONNECTOR_VERSION);
    jsonObject.addProperty("datasource", dataSourceVersion);
    jsonObject.addProperty("dataprocImage", DATAPROC_IMAGE_PART);
    jsonObject.addProperty("gcpRegion", GCP_REGION_PART);
    jsonObject.addProperty("sparkVersion", SPARK_VERSION);
    jsonObject.addProperty("javaVersion", JAVA_VERSION);
    jsonObject.addProperty("scalaVersion", SCALA_VERSION);
    gpn.ifPresent(s -> jsonObject.addProperty("GPN", s));
    return jsonObject.toString();
  }
}
