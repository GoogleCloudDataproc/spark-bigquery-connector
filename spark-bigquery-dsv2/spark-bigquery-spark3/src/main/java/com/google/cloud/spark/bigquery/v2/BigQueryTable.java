package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

// extends Dataset<Row>
public class BigQueryTable implements SupportsRead, SupportsWrite {
  private Set<TableCapability> capabilities;
  private final StructType schema;
  private final Transform[] partitioning;
  private final Map<String, String> properties;
  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final SparkSession spark;

  @Override
  public Transform[] partitioning() {
    return this.partitioning;
  }

  public BigQueryTable(
      StructType schema,
      Transform[] partitioning,
      Map<String, String> properties,
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      SparkSession spark)
      throws AnalysisException {
    this.schema = schema;
    this.partitioning = partitioning;
    this.properties = properties;
    this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.spark = spark;
  }

  public DataType getDataType(String type, long precision, long scale) {
    switch (type) {
      case "INTEGER":
        return DataTypes.IntegerType;
      case "DOUBLE":
        return DataTypes.DoubleType;
      case "FLOAT":
        return DataTypes.FloatType;
      case "NUMERIC":
        return DataTypes.createDecimalType((int) precision, (int) scale);
      case "VARCHAR":
      case "CHAR":
      case "STRING":
      default:
        return DataTypes.StringType;
    }
  }

  @Override
  public Map<String, String> properties() {
    return this.properties;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Map<String, String> props = new HashMap<>(options);
    Injector injector = createInjector(null, props, new BigQueryScanBuilderModule());
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table == null) {
      return null;
    }
    int schemaLen = (int) table.getDefinition().getSchema().getFields().stream().count();
    StructField[] structFields = new StructField[schemaLen];
    for (int i = 0; i < schemaLen; i++) {
      Field field = table.getDefinition().getSchema().getFields().get(i);
      StructField structField =
          new StructField(
              field.getName(),
              getDataType(
                  field.getType().name(),
                  field.getPrecision() == null ? 0L : field.getPrecision(),
                  field.getScale() == null ? 0L : field.getScale()),
              true,
              null);
      structFields[i] = structField;
    }
    injector = createInjector(new StructType(structFields), props, new BigQueryScanBuilderModule());
    BigQueryScanBuilder bqScanBuilder = injector.getInstance(BigQueryScanBuilder.class);
    return bqScanBuilder;
  }

  @Override
  public String name() {
    return this.config.getTableId().getTable();
  }

  @Override
  public StructType schema() {
    return new StructType(this.schema.fields());
  }

  @Override
  public Set<TableCapability> capabilities() {
    if (capabilities == null) {
      capabilities = new HashSet<TableCapability>();
      capabilities.add(TableCapability.BATCH_READ);
      capabilities.add(TableCapability.BATCH_WRITE);
      capabilities.add(TableCapability.TRUNCATE);
      capabilities.add(TableCapability.OVERWRITE_BY_FILTER);
      capabilities.add(TableCapability.OVERWRITE_DYNAMIC);
    }
    return capabilities;
  }

  // This method is used to create spark session
  public SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }

  // This method is used to create injection by providing
  public Injector createInjector(StructType schema, Map<String, String> options, Module module) {
    SparkSession spark = getDefaultSparkSessionOrCreate();
    return Guice.createInjector(
        new BigQueryClientModule(),
        new SparkBigQueryConnectorModule(
            spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
        module);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    Map<String, String> props = new HashMap<>(logicalWriteInfo.options());
    Injector injector =
        createInjector(
            logicalWriteInfo.schema(),
            props,
            new BigQueryDataSourceWriterModule(
                logicalWriteInfo.queryId(),
                logicalWriteInfo.schema(),
                SaveMode.Append,
                logicalWriteInfo,
                this.bigQueryClient,
                this.config));
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    BigQueryWriteBuilder writer = injector.getInstance(BigQueryWriteBuilder.class);
    return writer;
  }
}
