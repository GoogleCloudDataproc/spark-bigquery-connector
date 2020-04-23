package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryStorageClientFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.VersionProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;

public class SparkBigQueryConnectorModule implements Module {

    private final SparkContext sparkContext;
    private final TableId tableId;
    private final ReadSessionCreatorConfig readSessionCreatorConfig;
    private final StructType schema;

    public SparkBigQueryConnectorModule(
            SparkContext sparkContext,
            TableId tableId,
            ReadSessionCreatorConfig readSessionCreatorConfig,
            StructType schema) {
        this.sparkContext = sparkContext;
        this.tableId = tableId;
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        this.schema = schema;
    }

    @Override
    public void configure(Binder binder) {
        // empty
    }

    @Singleton
    @Provides
    public VersionProvider provideVersionProvider() {
        return new SparkBigQueryConnectorVersionProvider(sparkContext);
    }

    @Singleton
    @Provides
    public BigQueryDataSourceReader provideDataSourceReader(BigQueryClient bigQueryClient,
                                                            BigQueryStorageClientFactory bigQueryStorageClientFactory) {
        return new BigQueryDataSourceReader(tableId,
                bigQueryClient,
                bigQueryStorageClientFactory,
                readSessionCreatorConfig,
                schema);
    }
}
