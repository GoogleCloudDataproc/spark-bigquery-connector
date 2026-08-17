/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClient.LoadDataOptions;
import com.google.common.collect.ImmutableList;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** The destination metadata that must be validated before a dynamic overwrite. */
public final class DestinationValidationOptions {

  private final TableId destinationTableId;
  private final boolean enableModeCheckForSchemaFields;
  private final Optional<String> partitionField;
  private final Optional<TimePartitioning.Type> partitionType;
  private final Optional<RangePartitioning.Range> partitionRange;
  private final OptionalLong partitionExpirationMs;
  private final Optional<Boolean> partitionRequireFilter;
  private final Optional<ImmutableList<String>> clusteredFields;

  /** Creates an immutable validation snapshot from the relevant load options. */
  public static DestinationValidationOptions from(LoadDataOptions options) {
    Objects.requireNonNull(options, "options");
    return new DestinationValidationOptions(
        options.getTableId(),
        options.getEnableModeCheckForSchemaFields(),
        options.getPartitionField(),
        options.getPartitionType(),
        options.getPartitionRange(),
        options.getPartitionExpirationMs(),
        options.getPartitionRequireFilter(),
        options.getClusteredFields());
  }

  private DestinationValidationOptions(
      TableId destinationTableId,
      boolean enableModeCheckForSchemaFields,
      Optional<String> partitionField,
      Optional<TimePartitioning.Type> partitionType,
      Optional<RangePartitioning.Range> partitionRange,
      OptionalLong partitionExpirationMs,
      Optional<Boolean> partitionRequireFilter,
      Optional<ImmutableList<String>> clusteredFields) {
    this.destinationTableId = Objects.requireNonNull(destinationTableId, "destinationTableId");
    this.enableModeCheckForSchemaFields = enableModeCheckForSchemaFields;
    this.partitionField = Objects.requireNonNull(partitionField, "partitionField");
    this.partitionType = Objects.requireNonNull(partitionType, "partitionType");
    this.partitionRange = Objects.requireNonNull(partitionRange, "partitionRange");
    this.partitionExpirationMs =
        Objects.requireNonNull(partitionExpirationMs, "partitionExpirationMs");
    this.partitionRequireFilter =
        Objects.requireNonNull(partitionRequireFilter, "partitionRequireFilter");
    this.clusteredFields =
        Objects.requireNonNull(clusteredFields, "clusteredFields").map(ImmutableList::copyOf);
  }

  public TableId getDestinationTableId() {
    return destinationTableId;
  }

  public boolean getEnableModeCheckForSchemaFields() {
    return enableModeCheckForSchemaFields;
  }

  public Optional<String> getPartitionField() {
    return partitionField;
  }

  public Optional<TimePartitioning.Type> getPartitionType() {
    return partitionType;
  }

  public TimePartitioning.Type getPartitionTypeOrDefault() {
    return partitionType.orElse(TimePartitioning.Type.DAY);
  }

  public Optional<RangePartitioning.Range> getPartitionRange() {
    return partitionRange;
  }

  public OptionalLong getPartitionExpirationMs() {
    return partitionExpirationMs;
  }

  public Optional<Boolean> getPartitionRequireFilter() {
    return partitionRequireFilter;
  }

  public Optional<ImmutableList<String>> getClusteredFields() {
    return clusteredFields;
  }
}
