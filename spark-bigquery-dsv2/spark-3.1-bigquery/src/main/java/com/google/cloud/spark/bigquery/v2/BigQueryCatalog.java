package com.google.cloud.spark.bigquery.v2;

import com.google.inject.Injector;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryCatalog implements CatalogExtension {

  private String name = "bigquery";
  private Injector injector;
  private CatalogPlugin delegate;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.injector =
        InjectorFactory.createInjector(/*schema*/ null, options, /* tableIsMandatory */ false);
  }

  @Override
  public void setDelegateCatalog(CatalogPlugin delegate) {
    this.delegate = delegate;
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return new String[0][];
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {}

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {}

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return BigQueryTable.fromIdentifier(injector, ident);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return null;
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
  public String name() {
    return name;
  }
}
