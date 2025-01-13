package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.InjectorBuilder;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spire.random.Op;

public class BigQueryTableCatalog implements TableCatalog {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryTableCatalog.class);
  private static final String[] DEFAULT_NAMESPACE = {"default"};

  TableProvider tableProvider;
  BigQueryClient bigQueryClient;
  SchemaConverters schemaConverters;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap caseInsensitiveStringMap) {
    logger.info("Initializing [{}])", name);
    // SparkBigQueryConfig config = SparkBigQueryConfig.from(ImmutableMap.of(), ImmutableMap.of(),
    // DataSourceVersion.V2, SparkSession.active(), Optional.empty(), false);
    Injector injector = new InjectorBuilder().withTableIsMandatory(false).build();
    tableProvider = StreamSupport.stream(ServiceLoader.load(DataSourceRegister.class).spliterator(),false)
            .filter(candidate -> candidate.shortName().equals("bigquery"))
            .map(candidate -> (TableProvider) candidate)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Could not find a BigQuery TableProvider"));
    bigQueryClient = injector.getInstance(BigQueryClient.class);
    schemaConverters = SchemaConverters.from(SchemaConvertersConfiguration.from(injector.getInstance(SparkBigQueryConfig.class)));
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
    return StreamSupport.stream(
            bigQueryClient.listTables(DatasetId.of(namespace[0]), TableDefinition.Type.TABLE).spliterator(), false)
        .map(com.google.cloud.bigquery.Table::getTableId)
        .map(BigQueryIdentifier::of)
        .toArray(BigQueryIdentifier[]::new);
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    logger.debug("loading table [{}])", format(identifier));
    return null;
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
    Schema schema = schemaConverters.toBigQuerySchema(structType);
    bigQueryClient.createTable(toTableId(identifier), schema, BigQueryClient.CreateTableOptions.of(Optional.empty(), ImmutableMap.of(), Optional.empty()));
    return tableProvider.getTable(structType, transforms, properties);
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... tableChanges)
      throws NoSuchTableException {
    throw new UnsupportedOperationException("Cannot alter table in BigQuery");
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    throw new UnsupportedOperationException("Cannot drop table in BigQuery");
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
