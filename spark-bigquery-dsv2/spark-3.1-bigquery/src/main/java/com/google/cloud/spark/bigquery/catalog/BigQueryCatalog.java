/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.catalog;

import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;
import static com.google.cloud.spark.bigquery.SchemaConverters.toSpark;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.InjectorBuilder;
import com.google.cloud.spark.bigquery.v2.BigQueryTable;
import com.google.common.collect.Streams;
import com.google.inject.Injector;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spark Catalog for BigQuery. */
public class BigQueryCatalog implements TableCatalog {

  private static final Logger log = LoggerFactory.getLogger(BigQueryCatalog.class);

  private BigQueryClient bqClient = null;
  private String catalogName = null;
  private CaseInsensitiveStringMap options = null;

  public BigQueryCatalog() {}

  /** For test only. */
  BigQueryCatalog(BigQueryClient bqClient) {
    this.bqClient = bqClient;
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    if (bqClient == null) {
      this.bqClient =
          new InjectorBuilder()
              .withOptions(options)
              .withTableIsMandatory(false)
              .build()
              .getInstance(BigQueryClient.class);
    }
    this.catalogName = name;
    this.options = options;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public BigQueryTable createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    TableId tableId = getTableId(ident);
    TableInfo bqTable = bqClient.createTable(tableId, toBigQuerySchema(schema));
    if (bqTable == null) {
      log.error("Unable to create BigQuery table {}", tableId);
      return null;
    }
    return toSparkTable(bqTable, properties);
  }

  @Override
  public BigQueryTable alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException(
        "BigQuery Spark Catalog does not support altering tables");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return bqClient.deleteTable(getTableId(ident));
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    List<Identifier> idList =
        Streams.stream(
                bqClient.listTables(
                    getDatasetId(namespace),
                    TableDefinition.Type.TABLE,
                    TableDefinition.Type.EXTERNAL,
                    TableDefinition.Type.VIEW,
                    TableDefinition.Type.MATERIALIZED_VIEW))
            .map(t -> Identifier.of(namespace, t.getTableId().getTable()))
            .collect(Collectors.toList());
    Identifier[] idArray = new Identifier[idList.size()];
    idArray = idList.toArray(idArray);
    return idArray;
  }

  @Override
  public BigQueryTable loadTable(Identifier ident) throws NoSuchTableException {
    TableId tableId = getTableId(ident);
    TableInfo bqTable = bqClient.getTable(tableId);
    if (bqTable == null) {
      log.info("Unable to get BigQuery table {}", tableId);
      return null;
    }
    return toSparkTable(bqTable, options);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    throw new UnsupportedOperationException(
        "BigQuery Spark Catalog does not support purging tables");
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException(
        "BigQuery Spark Catalog does not support renaming tables");
  }

  private static DatasetId getDatasetId(String[] namespace) {
    if (namespace.length != 1) {
      throw new IllegalArgumentException(
          String.format(
              "BigQuery uses dataset as single level namespace, found invalid namespace: %s",
              namespace));
    }
    return DatasetId.of(namespace[0]);
  }

  private static TableId getTableId(Identifier ident) {
    return TableId.of(getDatasetId(ident.namespace()).getDataset(), ident.name());
  }

  private static Injector createInjector(StructType schema, Map<String, String> options) {
    return new InjectorBuilder()
        .withOptions(options)
        .withSchema(schema)
        .withTableIsMandatory(false)
        .withDataSourceVersion(DataSourceVersion.V2)
        .build();
  }

  private static BigQueryTable toSparkTable(TableInfo bqTable, Map<String, String> options) {
    StructType schema = toSpark(bqTable.getDefinition().getSchema());
    return BigQueryTable.fromIdentifier(
        createInjector(schema, options), bqTable.getTableId(), schema);
  }
}
