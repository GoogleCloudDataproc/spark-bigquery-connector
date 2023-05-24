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

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Suppliers;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * The original indirect insertable relation, using Spark's write. Intermediate formats are Parquet,
 * ORC or Avro. Deprecated in favor of BigQueryDataSourceWriterInsertableRelation.
 */
public class BigQueryDeprecatedIndirectInsertableRelation extends BigQueryInsertableRelationBase {

  public BigQueryDeprecatedIndirectInsertableRelation(
      BigQueryClient bigQueryClient, SQLContext sqlContext, SparkBigQueryConfig config) {
    super(bigQueryClient, sqlContext, config);
  }

  @Override
  public void insert(Dataset<Row> data, boolean overwrite) {
    logger.debug("Inserting data={}, overwrite={}", data, overwrite);
    // the helper also supports the v2 api
    SaveMode saveMode = overwrite ? SaveMode.Overwrite : SaveMode.Append;
    BigQueryWriteHelper helper =
        new BigQueryWriteHelper(
            bigQueryClient, sqlContext, saveMode, config, data, Optional.ofNullable(table.get()));
    helper.writeDataFrameToBigQuery();
    table = Suppliers.memoize(() -> bigQueryClient.getTable(config.getTableId()));
  }
}
