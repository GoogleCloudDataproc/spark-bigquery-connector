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
package com.google.cloud.spark.bigquery.write;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Suppliers;
import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BigQueryInsertableRelationBase extends BaseRelation
    implements InsertableRelation {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected final BigQueryClient bigQueryClient;
  protected final SQLContext sqlContext;
  protected final SparkBigQueryConfig config;
  protected Supplier<TableInfo> table;

  protected BigQueryInsertableRelationBase(
      BigQueryClient bigQueryClient, SQLContext sqlContext, SparkBigQueryConfig config) {
    this.bigQueryClient = bigQueryClient;
    this.sqlContext = sqlContext;
    this.config = config;
    this.table = Suppliers.memoize(() -> bigQueryClient.getTable(config.getTableId()));
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public StructType schema() {
    return SchemaConverters.from(SchemaConvertersConfiguration.from(config))
        .toSpark(table.get().getDefinition().getSchema());
  }

  /** Does this table exist? */
  public boolean exists() {
    return table.get() != null;
  }

  /** Is this table empty? A none-existing table is considered to be empty */
  public boolean isEmpty() {
    return numberOfRows().map(n -> n.longValue() == 0).orElse(true);
  }

  /** Returns the number of rows in the table. If the table does not exist return None */
  private Optional<BigInteger> numberOfRows() {
    return Optional.of(table.get().getNumRows());
  }

  public TableId getTableId() {
    return config.getTableId();
  }
}
