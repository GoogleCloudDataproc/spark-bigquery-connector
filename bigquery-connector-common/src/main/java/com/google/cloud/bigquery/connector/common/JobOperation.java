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
package com.google.cloud.bigquery.connector.common;

/**
 * Enum representing different BigQuery job operations tracked by the connector. Used for telemetry
 * and logging purposes.
 */
public enum JobOperation {
  /** Operation for loading data from GCS to a BigQuery table. */
  LOAD_DATA_FROM_GCS("Load Data from GCS to Table"),
  /** Operation for materializing a query result into a BigQuery table. */
  MATERIALIZE_QUERY("Materialize Query to Table"),
  /** Operation for committing an overwrite transaction. */
  OVERWRITE_TRANSACTION("Overwrite Transaction Commit"),
  /** Operation for committing an append transaction. */
  APPEND_TRANSACTION("Append Transaction Commit"),
  /** Operation for committing a dynamic partition overwrite. */
  DYNAMIC_PARTITION_OVERWRITE("Dynamic Partition Overwrite Commit");

  private final String description;

  JobOperation(String description) {
    this.description = description;
  }

  /**
   * Returns the user-friendly description of the job operation.
   *
   * @return the description of the operation
   */
  public String getDescription() {
    return description;
  }
}
