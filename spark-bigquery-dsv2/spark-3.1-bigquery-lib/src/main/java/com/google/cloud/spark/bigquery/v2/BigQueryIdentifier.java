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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import org.apache.spark.sql.connector.catalog.Identifier;

public class BigQueryIdentifier implements Identifier {

  private final TableId tableId;

  public BigQueryIdentifier(TableId tableId) {
    this.tableId = tableId;
  }

  @Override
  public String[] namespace() {
    return tableId.getProject() == null
        ? new String[] {tableId.getDataset()}
        : new String[] {tableId.getProject(), tableId.getDataset()};
  }

  @Override
  public String name() {
    return tableId.getTable();
  }

  public TableId getTableId() {
    return tableId;
  }

  @Override
  public String toString() {
    return "BigQueryIdentifier{" + "tableId=" + BigQueryUtil.friendlyTableName(tableId) + '}';
  }
}
