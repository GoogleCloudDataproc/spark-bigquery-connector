/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class BigQueryConfigurationUtilTest {

  private static final ImmutableMap<String, String> EMPTY_GLOBAL_OPTIONS =
      ImmutableMap.<String, String>of();

  @Test
  public void testParseSimpleTableId_tableOnly() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            EMPTY_GLOBAL_OPTIONS, ImmutableMap.of("table", "project.dataset.table"));
    assertThat(result.getProject()).isEqualTo("project");
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_pathOnly() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            EMPTY_GLOBAL_OPTIONS, ImmutableMap.of("path", "project.dataset.table"));
    assertThat(result.getProject()).isEqualTo("project");
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_tableAndDataset() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            EMPTY_GLOBAL_OPTIONS, ImmutableMap.of("table", "table", "dataset", "dataset"));
    assertThat(result.getProject()).isNull();
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_allParams() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            EMPTY_GLOBAL_OPTIONS,
            ImmutableMap.of("table", "table", "dataset", "dataset", "project", "project"));
    assertThat(result.getProject()).isEqualTo("project");
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_fallbackDatasetIgnored() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            ImmutableMap.of("materializationDataset", "fallback"),
            ImmutableMap.of("table", "table", "dataset", "dataset"));
    assertThat(result.getProject()).isNull();
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_fallbackDatasetUsed() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            ImmutableMap.of("materializationDataset", "fallback"),
            ImmutableMap.of("table", "table"));
    assertThat(result.getProject()).isNull();
    assertThat(result.getDataset()).isEqualTo("fallback");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_fallbackProjectIgnored() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            ImmutableMap.of("materializationProject", "fallback"),
            ImmutableMap.of("table", "table", "dataset", "dataset", "project", "project"));
    assertThat(result.getProject()).isEqualTo("project");
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_fallbackProjectUsed() {
    TableId result =
        BigQueryConfigurationUtil.parseSimpleTableId(
            ImmutableMap.of("materializationProject", "fallback"),
            ImmutableMap.of("table", "table", "dataset", "dataset"));
    assertThat(result.getProject()).isEqualTo("fallback");
    assertThat(result.getDataset()).isEqualTo("dataset");
    assertThat(result.getTable()).isEqualTo("table");
  }

  @Test
  public void testParseSimpleTableId_missingDataset() {
    assertThrows(
        "Missing dataset exception was not thrown",
        IllegalArgumentException.class,
        () -> {
          BigQueryConfigurationUtil.parseSimpleTableId(
              EMPTY_GLOBAL_OPTIONS, ImmutableMap.of("table", "table"));
        });
  }
}
