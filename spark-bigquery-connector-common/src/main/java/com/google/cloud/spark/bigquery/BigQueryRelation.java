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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;

public class BigQueryRelation extends BaseRelation {

  private final SparkBigQueryConfig options;
  private final TableInfo table;
  private final SQLContext sqlContext;
  private final TableId tableId;
  private final String tableName;

  public BigQueryRelation(SparkBigQueryConfig options, TableInfo table, SQLContext sqlContext) {
    this.options = options;
    this.table = table;
    this.sqlContext = sqlContext;
    this.tableId = table.getTableId();
    this.tableName = BigQueryUtil.friendlyTableName(tableId);
  }

  @Override
  public SQLContext sqlContext() {
    return this.sqlContext;
  }

  @Override
  public StructType schema() {
    SchemaConverters sc = SchemaConverters.from(SchemaConvertersConfiguration.from(options));
    return options.getSchema().orElse(sc.toSpark(sc.getSchemaWithPseudoColumns(table)));
  }

  public TableId getTableId() {
    return tableId;
  }

  public String getTableName() {
    return tableName;
  }
}
