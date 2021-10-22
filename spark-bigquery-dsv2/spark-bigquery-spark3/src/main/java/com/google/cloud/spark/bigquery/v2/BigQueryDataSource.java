package com.google.cloud.spark.bigquery.v2;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryDataSource implements TableProvider, DataSourceRegister, SupportsRead {

  private Set<TableCapability> capabilities;

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return null;
  }

  @Override
  public String shortName() {
    return "bigquery";
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public String name() {
    return this.getClass().getName();
  }

  // Returns the schema of this table. If the table is not readable and doesn't have a schema, an
  // empty schema can be returned here.
  @Override
  public StructType schema() {
    return null;
  }

  // Returns the set of capabilities for this table. E.g. Our DataSource Connector only supports
  // batch read for now
  @Override
  public Set<TableCapability> capabilities() {
    if (capabilities == null) {
      capabilities = new HashSet<TableCapability>();
      capabilities.add(TableCapability.BATCH_READ);
      capabilities.add(TableCapability.BATCH_WRITE);
    }
    return capabilities;
  }
}
