package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryDataSourceV2 implements TableProvider, DataSourceRegister {

  private static CaseInsensitiveStringMap options;

  @Override
  public StructType inferSchema(final CaseInsensitiveStringMap options) {
    if (options.get("schema") != null) {
      return getTable(StructType.fromDDL(options.get("schema")), null, options.asCaseSensitiveMap())
          .schema();
    }
    //    return getTable(null, null, options.asCaseSensitiveMap()).schema();
    StructField[] structFields = new StructField[0];
    return getTable(new StructType(structFields), null, options).schema();
    //    StructField[] structFields = new StructField[0];
    //    return new StructType(structFields);
    //    return null;
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
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    Map<String, String> props = new HashMap<>(properties);
    Injector injector;
    injector = createInjector(schema, props, new BigQueryTableModule(schema, props));
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      schema = SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
      injector = createInjector(schema, props, new BigQueryTableModule(schema, props));
      bigQueryClient = injector.getInstance(BigQueryClient.class);
      config = injector.getInstance(SparkBigQueryConfig.class);
    }
    return injector.getInstance(BigQueryTable.class);
  }

  @Override
  public String shortName() {
    return "bigquery";
  }

  // This method is used to create spark session
  public SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
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
}
