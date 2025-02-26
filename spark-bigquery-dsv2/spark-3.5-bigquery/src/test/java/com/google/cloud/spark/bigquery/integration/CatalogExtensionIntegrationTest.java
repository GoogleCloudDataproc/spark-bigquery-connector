/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.List;


import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class CatalogExtensionIntegrationTest extends CatalogIntegrationTestBase {

  @Override
  protected SparkSession createSparkSession() {
    return SparkSession.builder()
        .appName("catalog extension test")
        .master("local[*]")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .config("spark.sql.sources.default", "bigquery")
        .config("spark.datasource.bigquery.writeMethod", "direct")
        .config("spark.sql.defaultCatalog", "bigquery")
        .config("spark.sql.catalog.bigquery", "com.google.cloud.spark.bigquery.BigQueryCatalog")
        .getOrCreate();
  }
}
