/*
         * Copyright 2018 Google Inc. All Rights Reserved.
         *
         * Licensed under the Apache License, Version 2.0 (the "License");
         * you may not use this file except in compliance with the License.
         * You may obtain a copy of the License at
         *
         * http://www.apache.org/licenses/LICENSE-2.0
         *
         * Unless required by applicable law or agreed to in writing, software
         * distributed under the License is distributed on an "AS IS" BASIS,
         * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
         * See the License for the specific language governing permissions and
         * limitations under the License.
         */
        package com.google.cloud.spark.bigquery;

import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * Static helpers for working with BigQuery, relevant only to the Scala code,
 * now translated for Java usage where applicable.
 */
public final class BigQueryUtilScala { // Renamed from BigQueryUtilScalaHelper to match original

  private BigQueryUtilScala() {} // Private constructor for utility class

  // validating that the connector's scala version and the runtime's scala
  // version are the same
  public static void validateScalaVersionCompatibility() {
    // In Java, this check might be different or rely on other mechanisms
    // if the primary concern is Spark's Scala binary compatibility.
    // For a direct translation of the Scala logic:
    String runtimeScalaVersion;
    try {
      // Attempt to get Scala version from properties if available in a pure Java context.
      // This is tricky as `scala.util.Properties.versionNumberString()` is Scala-specific.
      // If running in a Spark environment, SparkContext might provide Scala version.
      // For a standalone Java library, this check might be omitted or re-thought.
      // As a placeholder, we'll assume it can be fetched or is less critical
      // when this specific method is called from Java.
      // If strictly needed, one might try to load Scala library classes dynamically.
      Class<?> scalaPropsClass = Class.forName("scala.util.Properties$");
      Object scalaPropsModule = scalaPropsClass.getField("MODULE$").get(null);
      runtimeScalaVersion = (String) scalaPropsClass.getMethod("versionNumberString").invoke(scalaPropsModule);
      runtimeScalaVersion = trimVersion(runtimeScalaVersion);
    } catch (Exception e) {
      // Could not determine runtime Scala version, or Scala libraries not present.
      // Depending on requirements, either log a warning, throw, or proceed.
      // For this translation, we'll indicate that it couldn't be determined.
      System.err.println("Warning: Could not determine runtime Scala version for compatibility check from Java.");
      // If this check is critical, the application might need to ensure Scala libraries are on the classpath
      // or handle this situation more gracefully.
      // For now, let's not throw an error to allow Java code to proceed.
      // This part is inherently difficult to translate perfectly without Scala runtime.
      return; // Or handle as an error
    }


    Properties buildProperties = new Properties();
    try (InputStream stream = BigQueryUtilScala.class.getResourceAsStream("/spark-bigquery-connector.properties")) {
      if (stream == null) {
        throw new IllegalStateException("Could not load spark-bigquery-connector.properties");
      }
      buildProperties.load(stream);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("Failed to load spark-bigquery-connector.properties", e);
    }

    String connectorScalaVersion = buildProperties.getProperty("scala.binary.version");

    if (connectorScalaVersion == null) {
      throw new IllegalStateException("Property 'scala.binary.version' not found in spark-bigquery-connector.properties");
    }

    if (runtimeScalaVersion != null && !runtimeScalaVersion.equals(connectorScalaVersion)) {
      throw new IllegalStateException(
              String.format("This connector was made for Scala %s, it was not meant to run on Scala %s",
                              connectorScalaVersion, runtimeScalaVersion)
                      .replace('\n', ' '));
    }
  }

  private static String trimVersion(String version) {
    return version.substring(0, version.lastIndexOf('.'));
  }

  public static <T> Optional<T> toOption(java.util.Optional<T> javaOptional) {
    // This method seems to convert a Java Optional to a Java Optional,
    // which is redundant. In Scala, it converted Java Optional to Scala Option.
    // If the intent is to ensure it's a Java Optional, the input is already that.
    // Perhaps it was meant for a different conversion in the Scala context.
    // For a direct Java equivalent of the Scala `toOption`, it would take a Java Optional
    // and return a Java Optional, effectively being an identity function for the type.
    return javaOptional;
  }
}