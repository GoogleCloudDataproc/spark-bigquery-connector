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

import com.google.cloud.spark.bigquery.v2.BaseBigQuerySource;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryCatalogExtension implements CatalogExtension, ViewCatalog {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryCatalogExtension.class);
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};

  private final BigQueryCatalog bigQueryCatalog = new BigQueryCatalog();
  private String name = "default";
  private CatalogPlugin sessionCatalog;
  private boolean isBigQueryTheDefaultProvider = false;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    logger.info("Initializing BigQuery session catalog [{}])", name);
    bigQueryCatalog.initialize(name, options);
    this.name = name;
    SQLConf sqlConf = SparkSession.active().sqlContext().conf();
    this.isBigQueryTheDefaultProvider =
        sqlConf.defaultDataSourceName().equals(BaseBigQuerySource.BIGQUERY_PROVIDER_NAME);
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
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return delegateAsSupportsNamespaces().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    delegateAsSupportsNamespaces().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    delegateAsSupportsNamespaces().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
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
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get("provider");
    if (isBigQuery(provider)) {
      return bigQueryCatalog.createTable(ident, schema, partitions, properties);
    } else {
      // delegate to the session catalog
      return delegateAsTableCatalog().createTable(ident, schema, partitions, properties);
    }
  }

  private boolean isBigQuery(String provider) {
    return isBigQueryTheDefaultProvider
        || BaseBigQuerySource.BIGQUERY_PROVIDER_NAME.equals(provider);
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
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {}

  @Override
  public Identifier[] listViews(String... namespace) throws NoSuchNamespaceException {
    return delegateAsViewCatalog().listViews(namespace);
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    return delegateAsViewCatalog().loadView(ident);
  }

  @Override
  public void invalidateView(Identifier ident) {
    delegateAsViewCatalog().invalidateView(ident);
  }

  @Override
  public boolean viewExists(Identifier ident) {
    return delegateAsViewCatalog().viewExists(ident);
  }

  @Override
  public View createView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    return delegateAsViewCatalog()
        .createView(
            ident,
            sql,
            currentCatalog,
            currentNamespace,
            schema,
            queryColumnNames,
            columnAliases,
            columnComments,
            properties);
  }

  @Override
  public View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    return delegateAsViewCatalog().alterView(ident, changes);
  }

  @Override
  public boolean dropView(Identifier ident) {
    return delegateAsViewCatalog().dropView(ident);
  }

  @Override
  public void renameView(Identifier oldIdent, Identifier newIdent)
      throws NoSuchViewException, ViewAlreadyExistsException {
    delegateAsViewCatalog().renameView(oldIdent, newIdent);
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

  private ViewCatalog delegateAsViewCatalog() {
    return (ViewCatalog) sessionCatalog;
  }
}
