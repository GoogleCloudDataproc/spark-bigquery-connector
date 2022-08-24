/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.util;

import com.google.common.collect.Streams;
import java.util.ServiceLoader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.immutable.Seq;

public abstract class ScalaUtils {
  private static ScalaUtils instance;

  public static ScalaUtils getInstance() {
    String scalaVersion = scala.util.Properties.versionNumberString();
    if (instance == null) {
      ServiceLoader<ScalaUtils> serviceLoader = ServiceLoader.load(ScalaUtils.class);
      instance =
          Streams.stream(serviceLoader.iterator())
              .filter(s -> s.supportsScalaVersion(scalaVersion))
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              "Could not load instance of [%], please check the META-INF/services directory in the connector's jar",
                              ScalaUtils.class.getCanonicalName())));
    }
    return instance;
  }

  public abstract boolean supportsScalaVersion(String scalaVersion);

  public abstract <T> Seq<T> emptySeq(Class<T> clazz);

  public abstract InternalRow rowToInternalRow(Row row);
}
