/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigquery.connector.common.BigQueryUtil;

import java.util.List;

/** Spark related utilities */
public class SparkBigQueryUtil {
  /**
   * Optimizing the URI list for BigQuery load, using the Spark specific file prefix and suffix
   * patterns, based on <code>BigQueryUtil.optimizeLoadUriList()</code>
   *
   * @param uris A list of URIs to be loaded by BigQuery load
   * @return an optimized list of URIs
   */
  public static List<String> optimizeLoadUriListForSpark(List<String> uris) {
    return BigQueryUtil.optimizeLoadUriList(uris, ".*/part-", "-[-\\w\\.]+");
  }
}
