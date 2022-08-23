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

import org.apache.spark.sql.Row;
import scala.collection.immutable.Seq;
import scala.collection.mutable.ListBuffer;

public class Scala213Utils extends ScalaUtils {

  @Override
  public boolean supportsScalaVersion(String scalaVersion) {
    return scalaVersion.compareTo("2.13") > 0;
  }

  @Override
  public Seq<Object> rowToSeq(Row row) {
    ListBuffer<Object> resultBuilder = new ListBuffer<>();
    resultBuilder.sizeHint(row.length());
    for (int i = 0; i < row.length(); i++) {
      resultBuilder.$plus$eq(row.get(i));
    }
    return resultBuilder.toSeq();
  }
}
