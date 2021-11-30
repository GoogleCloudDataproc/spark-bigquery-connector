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
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BigQueryDataSourceHelper {
  private enum WriteMethod {
    DIRECT("direct"),
    INDIRECT("indirect");

    private final String writePath;

    WriteMethod(String writePath) {
      this.writePath = writePath;
    }

    static WriteMethod getWriteMethod(Optional<String> path) {
      if (!path.isPresent() || path.get().equalsIgnoreCase("direct")) {
        return DIRECT;
      } else if (path.get().equalsIgnoreCase("indirect")) {
        return INDIRECT;
      } else {
        throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
      }
    }

    static WriteMethod getWriteMethod(String path) {
      if (path == null || path.equalsIgnoreCase("direct")) {
        return DIRECT;
      } else if (path.equalsIgnoreCase("indirect")) {
        return INDIRECT;
      } else {
        throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
      }
    }
  }

  private boolean isDirectWrite = false;
  private StructType schema;
  private boolean tableExists = false;

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

  public Injector createInjector(
      StructType schema,
      Map<String, String> options,
      boolean isDirectWrite,
      SaveMode mode,
      Module module) {
    SparkSession spark = getDefaultSparkSessionOrCreate();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new SparkBigQueryConnectorModule(
                spark, options, Optional.ofNullable(schema), DataSourceVersion.V2),
            module);
    return getTableInformation(injector, mode, isDirectWrite);
  }

  public Injector getTableInformation(Injector injector, SaveMode mode, boolean isDirectWrite) {
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableInfo table = bigQueryClient.getTable(config.getTableId());
    if (table != null) {
      this.tableExists = true;
      if (mode != null && !isDirectWrite) {
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

  public boolean isDirectWrite(Optional<String> writeType) {
    WriteMethod path = WriteMethod.getWriteMethod(writeType);
    if (path.equals(WriteMethod.DIRECT)) {
      this.isDirectWrite = true;
      return true;
    } else if (path.equals(WriteMethod.INDIRECT)) {
      this.isDirectWrite = false;
      return false;
    } else {
      throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
    }
  }

  public boolean isDirectWrite(String writeType) {
    WriteMethod path = WriteMethod.getWriteMethod(writeType);
    if (path.equals(WriteMethod.DIRECT)) {
      this.isDirectWrite = true;
      return true;
    } else if (path.equals(WriteMethod.INDIRECT)) {
      this.isDirectWrite = false;
      return false;
    } else {
      throw new IllegalArgumentException("Unknown writePath Provided for writing the DataFrame");
    }
  }

  public boolean isDirectWrite() {
    return isDirectWrite;
  }

  public boolean isTableExists() {
    return tableExists;
  }
}
