package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class BigQueryDataSourceReaderModule implements Module {
    @Override
    public void configure(Binder binder) {
        // empty
    }

    @Singleton
    @Provides
    public BigQueryDataSourceReader provideDataSourceReader(
            BigQueryClient bigQueryClient,
            BigQueryReadClientFactory bigQueryReadClientFactory,
            SparkBigQueryConfig config) {
        TableInfo tableInfo =
                bigQueryClient.getSupportedTable(
                        config.getTableId(), config.isViewsEnabled(), SparkBigQueryConfig.VIEWS_ENABLED_OPTION);
        return new BigQueryDataSourceReader(
                tableInfo,
                bigQueryClient,
                bigQueryReadClientFactory,
                config.toReadSessionCreatorConfig(),
                config.getFilter(),
                config.getSchema());
    }

}
