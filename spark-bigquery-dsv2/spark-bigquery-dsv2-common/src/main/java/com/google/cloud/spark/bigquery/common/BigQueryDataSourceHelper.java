package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

public class BigQueryDataSourceHelper {
  private StructType schema;

  public StructType getSchema() {
    return schema;
  }

  public SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getActiveSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }

  // This method is used to create injection by providing
  public Injector createInjector(
      StructType schema, DataSourceOptions options, SaveMode mode, Module module) {
    SparkSession spark = getDefaultSparkSessionOrCreate();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new SparkBigQueryConnectorModule(
                spark, options.asMap(), Optional.ofNullable(schema), DataSourceVersion.V2),
            module);
    return getTableInformation(injector, null);
  }

  public Injector getTableInformation(Injector injector, SaveMode mode) {
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      if (mode != null) {
        if (mode == SaveMode.Ignore) {
          //                    return Optional.empty();
        }
        if (mode == SaveMode.ErrorIfExists) {
          throw new IllegalArgumentException(
              String.format(
                  "SaveMode is set to ErrorIfExists and table '%s' already exists. Did you want "
                      + "to add data to the table by setting the SaveMode to Append? Example: "
                      + "df.write.format.options.mode(\"append\").save()",
                  BigQueryUtil.friendlyTableName(table.getTableId())));
        }
      } else {
        this.schema = SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
      }
    } else {
      // table does not exist
      // If the CreateDisposition is CREATE_NEVER, and the table does not exist,
      // there's no point in writing the data to GCS in the first place as it going
      // to fail on the BigQuery side.
      new GenericDataSourceHelperClass().checkCreateDisposition(config);
    }
    return injector;
  }
}
