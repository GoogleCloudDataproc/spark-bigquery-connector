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
import com.google.cloud.spark.bigquery.v2.BaseBigQuerySource;
import com.google.cloud.spark.bigquery.v2.BigQueryIdentifier;
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTable;
import com.google.cloud.spark.bigquery.v2.Spark3Util;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySessionCatalog implements CatalogExtension {

  private static final Logger logger = LoggerFactory.getLogger(BigQuerySessionCatalog.class);
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};

  private final BigQueryCatalog bigQueryCatalog = new BigQueryCatalog();
  private String name = "default";
  private CatalogPlugin sessionCatalog;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    logger.info("Initializing BigQuery session catalog [{}])", name);
    bigQueryCatalog.initialize(name, options);
    this.name = name;

  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String[] defaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  @Override
  public void setDelegateCatalog(CatalogPlugin delegate) {
    this.sessionCatalog = delegate;
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return delegateAsFunctionCatalog().listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return delegateAsFunctionCatalog().loadFunction(ident);
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return delegateAsSupportsNamespaces().listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return delegateAsSupportsNamespaces().listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    return delegateAsSupportsNamespaces().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    delegateAsSupportsNamespaces().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
delegateAsSupportsNamespaces().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return delegateAsSupportsNamespaces().dropNamespace(namespace, cascade);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      return bigQueryCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return delegateAsTableCatalog().loadTable(ident);
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    if (BaseBigQuerySource.BIGQUERY_PROVIDER_NAME.equalsIgnoreCase(provider)) {
      return bigQueryCatalog.createTable(ident, schema, partitions, properties);
    } else {
      // delegate to the session catalog
      return delegateAsTableCatalog().createTable(ident, schema, partitions, properties);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return null;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return false;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {

  }

  private TableCatalog delegateAsTableCatalog() {
    return (TableCatalog) sessionCatalog;
  }
  private FunctionCatalog delegateAsFunctionCatalog() {
    return (FunctionCatalog) sessionCatalog;
  }
  private SupportsNamespaces delegateAsSupportsNamespaces() {
    return (SupportsNamespaces) sessionCatalog;
  }
}
