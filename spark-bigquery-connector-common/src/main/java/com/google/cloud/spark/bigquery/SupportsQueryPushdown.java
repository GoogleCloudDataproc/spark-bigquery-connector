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

package com.google.cloud.spark.bigquery;

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;
import java.util.Optional;

public interface SupportsQueryPushdown {
  BigQueryRDDFactory getBigQueryRDDFactory();

  // Spark 3.1 DataSourceV2 connector does not create a Filter node in the logical plan. Instead,
  // we get all the filters from the ScanBuilder
  Optional<String> getPushdownFilters();
}
