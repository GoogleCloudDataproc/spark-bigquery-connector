/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.spark.bigquery.v2.BigQueryIdentifier;
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTable;
import com.google.cloud.spark.bigquery.v2.Spark3Util;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryCatalog implements TableCatalog {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryCatalog.class);
  private static final String[] DEFAULT_NAMESPACE = {"default"};

  private static final Cache<String, Table> identifierToTableCache =
      CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.MINUTES).maximumSize(1000).build();

  private TableProvider tableProvider;
  private BigQueryClient bigQueryClient;
  private SchemaConverters schemaConverters;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap caseInsensitiveStringMap) {
    logger.info("Initializing BigQuery table catalog [{}])", name);
    Injector injector = new InjectorBuilder().withTableIsMandatory(false).build();
    tableProvider =
        StreamSupport.stream(ServiceLoader.load(DataSourceRegister.class).spliterator(), false)
            .filter(candidate -> candidate.shortName().equals("bigquery"))
            .map(candidate -> (TableProvider) candidate)
            .findFirst()
            .orElseThrow(
                () -> new IllegalStateException("Could not find a BigQuery TableProvider"));
    bigQueryClient = injector.getInstance(BigQueryClient.class);
    schemaConverters =
        SchemaConverters.from(
            SchemaConvertersConfiguration.from(injector.getInstance(SparkBigQueryConfig.class)));
  }

  @Override
  public String name() {
    return "bigquery";
  }

  @Override
  public String[] defaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    Preconditions.checkNotNull(namespace, "namespace cannot be null");
    Preconditions.checkArgument(namespace.length == 1, "BigQuery supports only one namespace");
    logger.debug("list tables [{}])", namespace[0]);
    DatasetId datasetId = DatasetId.of(namespace[0]);
    try {
      return StreamSupport.stream(
              bigQueryClient.listTables(datasetId, TableDefinition.Type.TABLE).spliterator(), false)
          .map(com.google.cloud.bigquery.Table::getTableId)
          .map(BigQueryIdentifier::of)
          .toArray(BigQueryIdentifier[]::new);
    } catch (BigQueryException e) {
      if (e.getCause() != null && e.getCause() instanceof GoogleJsonResponseException) {
        GoogleJsonResponseException gsre = (GoogleJsonResponseException) e.getCause();
        if (gsre.getStatusCode() == 404) {
          // the dataset does not exist
          logger.debug("Dataset does not exist", e);
          throw new NoSuchNamespaceException(namespace);
        }
      }
      throw new BigQueryConnectorException(
          "Error listing tables  in " + Arrays.toString(namespace), e);
    }
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    logger.debug("loading table [{}])", format(identifier));
    TableId tableId = toTableId(identifier);
    try {
      return identifierToTableCache.get(
          identifier.toString(),
          () ->
              // TODO: reuse injector
              Spark3Util.createBigQueryTableInstance(
                  Spark35BigQueryTable::new,
                  null,
                  ImmutableMap.of(
                          "project", tableId.getProject(),
                          "dataset", tableId.getDataset(),
                          "table", tableId.getTable())));
    } catch (ExecutionException e) {
      throw new BigQueryConnectorException("Problem loaing table " + identifier, e);
    }
  }

  @Override
  public void invalidateTable(Identifier identifier) {
    logger.debug("invalidating table [{}])", format(identifier));
    throw new UnsupportedOperationException("Cannot invalidate table in BigQuery");
  }

  @Override
  public boolean tableExists(Identifier identifier) {
    logger.debug("checking existence of table [{}])", format(identifier));
    return bigQueryClient.tableExists(toTableId(identifier));
  }

  @Override
  public Table createTable(
      Identifier identifier,
      StructType structType,
      Transform[] transforms,
      Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    logger.debug("creating table [{}])", format(identifier));
    if (tableExplicitlySet(properties)) {
      logger.debug("Mapping Spark table to BigQuery table)");
      // As the table is mapped to an actual table in BigQuery, we are relying on the BigQuery
      // schema
      try {
        return identifierToTableCache.get(
            identifier.toString(),
            () ->
                Spark3Util.createBigQueryTableInstance(
                    Spark35BigQueryTable::new, /* schema */ null, properties));
      } catch (ExecutionException e) {
        throw new BigQueryConnectorException("Error creating table " + identifier, e);
      }
    }
    Schema schema = schemaConverters.toBigQuerySchema(structType);
    bigQueryClient.createTable(
        toTableId(identifier),
        schema,
        BigQueryClient.CreateTableOptions.of(
            Optional.empty(), ImmutableMap.of(), Optional.empty()));
    ImmutableMap.Builder<String, String> getTableProperties =
        ImmutableMap.<String, String>builder().putAll(properties);
    // if the user provided an alternative table we do not want to ignore it
    if (!tableExplicitlySet(properties)) {
      getTableProperties.put("dataset", identifier.namespace()[0]).put("table", identifier.name());
    }
    // TODO: Use the table constructor directly using the catalog's injector
    return tableProvider.getTable(structType, transforms, getTableProperties.buildKeepingLast());
  }

  private static boolean tableExplicitlySet(Map<String, String> properties) {
    if (properties.containsKey("table")) {
      return true;
    }
    if (properties.containsKey("path")) {
      return true;
    }
    return false;
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... tableChanges)
      throws NoSuchTableException {
    throw new UnsupportedOperationException("Cannot alter table in BigQuery");
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    logger.debug("dropping table [{}])", format(identifier));
    TableId tableId = toTableId(identifier);
    if (!bigQueryClient.tableExists(tableId)) {
      return false;
    }
    if (bigQueryClient.deleteTable(tableId)) {
      identifierToTableCache.invalidate(identifier.toString());
      return true;
    }
    return false;
  }

  @Override
  public boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Cannot purge table in BigQuery");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Cannot rename table in BigQuery");
  }

  private String format(Identifier identifier) {
    return String.format("%s.%s", Arrays.toString(identifier.namespace()), identifier.name());
  }

  static TableId toTableId(Identifier identifier) {
    if (identifier.namespace().length == 1) {
      return TableId.of(identifier.namespace()[0], identifier.name());
    }
    if (identifier.namespace().length == 2) {
      return TableId.of(identifier.namespace()[0], identifier.namespace()[1], identifier.name());
    }
    throw new IllegalArgumentException(
        "The identifier [" + identifier + "] is not recognized by BigQuery");
  }
}
