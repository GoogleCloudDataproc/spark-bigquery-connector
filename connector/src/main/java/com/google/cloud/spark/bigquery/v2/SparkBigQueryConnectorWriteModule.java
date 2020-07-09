package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryConnectorUserAgentProvider;
import com.google.cloud.spark.bigquery.SparkBigQueryWriteConfig;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

import static scala.collection.JavaConversions.mapAsJavaMap;

public class SparkBigQueryConnectorWriteModule implements Module {
    private final String writeUUID;
    private final SparkSession spark;
    private final StructType schema;
    private final SaveMode saveMode;
    private final DataSourceOptions options;

    public SparkBigQueryConnectorWriteModule(
            String writeUUID, SparkSession spark, StructType schema, SaveMode saveMode, DataSourceOptions options) {
        this.writeUUID = writeUUID;
        this.spark = spark;
        this.saveMode = saveMode;
        this.schema = schema;
        this.options = options;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(BigQueryConfig.class).toProvider(this::provideSparkBigQueryWriteConfig);
    }

    @Singleton
    @Provides
    public SparkBigQueryWriteConfig provideSparkBigQueryWriteConfig() {
        return SparkBigQueryWriteConfig.from(
                schema,
                saveMode,
                options,
                writeUUID,
                spark.sparkContext().hadoopConfiguration(),
                spark.sparkContext().defaultParallelism(),
                ImmutableMap.copyOf(mapAsJavaMap(spark.conf().getAll()))
                );
    }

    @Singleton
    @Provides
    public BigQueryDataSourceWriter provideDataSourceWriter(
            BigQueryClientForWriting bigQueryClient,
            BigQueryWriteClientFactory bigQueryWriteClientFactory,
            SparkBigQueryWriteConfig config) {
        TableInfo tableInfo = config.getTableInfo();
        return new BigQueryDataSourceWriter(
                tableInfo,
                bigQueryClient,
                bigQueryWriteClientFactory,
                config.toWriteSessionCreatorConfig());
    }

    @Singleton
    @Provides
    public UserAgentProvider provideUserAgentProvider() {
        return new SparkBigQueryConnectorUserAgentProvider("v2");
    }
}
