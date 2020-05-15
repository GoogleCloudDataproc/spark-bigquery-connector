package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryStorageClientFactory;
import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

import static scala.collection.JavaConversions.mapAsJavaMap;

public class SparkBigQueryConnectorModule implements Module {

    private final SparkSession spark;
    private final DataSourceOptions options;
    private final Optional<StructType> schema;

    public SparkBigQueryConnectorModule(
            SparkSession spark,
            DataSourceOptions options,
            Optional<StructType> schema) {
        this.spark = spark;
        this.options = options;
        this.schema = schema;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(UserAgentProvider.class).to(SparkBigQueryConnectorUserAgentProvider.class).in(Singleton.class);
        binder.bind(BigQueryConfig.class).toProvider(this::provideSparkBigQueryConfig);
    }

    @Singleton
    @Provides
    public SparkBigQueryConfig provideSparkBigQueryConfig() {
        return SparkBigQueryConfig.from(options,
                ImmutableMap.copyOf(mapAsJavaMap(spark.conf().getAll())),
                spark.sparkContext().hadoopConfiguration(),
                spark.sparkContext().defaultParallelism());
    }

    @Singleton
    @Provides
    public BigQueryDataSourceReader provideDataSourceReader(
            BigQueryClient bigQueryClient,
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            SparkBigQueryConfig config) {
        TableInfo tableInfo = bigQueryClient.getSupportedTable(config.getTableId(), config.isViewsEnabled(),
                SparkBigQueryConfig.VIEWS_ENABLED_OPTION);
        return new BigQueryDataSourceReader(tableInfo,
                bigQueryClient,
                bigQueryStorageClientFactory,
                config.toReadSessionCreatorConfig(),
                config.getFilter(),
                schema);
    }
}
