/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Test;

public class BigQueryRelationTest {

  @Test
  public void testCreateTableNameForLogging_withTable() {
    BigQueryRelation relation =
        new BigQueryRelation(
            SparkBigQueryConfig.from(
                ImmutableMap.of("path", "dataset.table"),
                ImmutableMap.of(),
                new Configuration(),
                ImmutableMap.of(),
                1,
                new SQLConf(),
                "1.2.3",
                Optional.empty(), /* tableIsMandatory */
                true),
            TableInfo.of(
                TableId.of("should", "not_be", "seen"), ViewDefinition.of("SELECT foo FROM bar")),
            null);
    String result = relation.getTableNameForLogging();
    assertThat(result).isEqualTo("dataset.table");
  }

  @Test
  public void testCreateTableNameForLogging_fromQuery() {
    BigQueryRelation relation =
        new BigQueryRelation(
            SparkBigQueryConfig.from(
                ImmutableMap.of("query", "SELECT foo FROM bar", "dataset", "dataset"),
                ImmutableMap.of(),
                new Configuration(),
                ImmutableMap.of(),
                1,
                new SQLConf(),
                "1.2.3",
                Optional.empty(), /* tableIsMandatory */
                true),
            TableInfo.of(
                TableId.of("project", "dataset", "_bqc_UUID"),
                StandardTableDefinition.of(Schema.of(Field.of("foo", LegacySQLTypeName.STRING)))),
            null);
    String result = relation.getTableNameForLogging();
    assertThat(result).isEqualTo("project.dataset._bqc_UUID created from \"SELECT foo FROM bar\"");
  }
}
