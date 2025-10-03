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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class MaterializationConfiguration {
  public static final int DEFAULT_MATERIALIZATION_EXPIRATION_TIME_IN_MINUTES = 24 * 60;

  private final com.google.common.base.Optional<String> materializationProject;
  private final com.google.common.base.Optional<String> materializationDataset;
  private final int materializationExpirationTimeInMinutes;

  public static MaterializationConfiguration from(
      ImmutableMap<String, String> globalOptions, Map<String, String> options) {
    com.google.common.base.Optional<String> materializationProject =
        BigQueryConfigurationUtil.getAnyOption(
            globalOptions,
            options,
            ImmutableList.of("materializationProject", "viewMaterializationProject"));
    com.google.common.base.Optional<String> materializationDataset =
        BigQueryConfigurationUtil.getAnyOption(
            globalOptions,
            options,
            ImmutableList.of("materializationDataset", "viewMaterializationDataset"));
    int materializationExpirationTimeInMinutes =
        BigQueryConfigurationUtil.getAnyOption(
                globalOptions, options, "materializationExpirationTimeInMinutes")
            .transform(Integer::parseInt)
            .or(DEFAULT_MATERIALIZATION_EXPIRATION_TIME_IN_MINUTES);
    if (materializationExpirationTimeInMinutes < 1) {
      throw new IllegalArgumentException(
          "materializationExpirationTimeInMinutes must have a positive value, the configured value is "
              + materializationExpirationTimeInMinutes);
    }

    return new MaterializationConfiguration(
        materializationProject, materializationDataset, materializationExpirationTimeInMinutes);
  }

  private MaterializationConfiguration(
      Optional<String> materializationProject,
      Optional<String> materializationDataset,
      int materializationExpirationTimeInMinutes) {
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
    this.materializationExpirationTimeInMinutes = materializationExpirationTimeInMinutes;
  }

  public Optional<String> getMaterializationProject() {
    return materializationProject;
  }

  public Optional<String> getMaterializationDataset() {
    return materializationDataset;
  }

  public int getMaterializationExpirationTimeInMinutes() {
    return materializationExpirationTimeInMinutes;
  }
}
