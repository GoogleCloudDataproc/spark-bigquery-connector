package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class GenericBigQueryDataSourceReader {

    private static final Logger logger = LoggerFactory.getLogger(GenericBigQueryDataSourceReader.class);
    private final TableInfo table;
    private final TableId tableId;
    private final ReadSessionCreatorConfig readSessionCreatorConfig;
    private final BigQueryClient bigQueryClient;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final BigQueryTracerFactory bigQueryTracerFactory;
    private final ReadSessionCreator readSessionCreator;
    private final Optional<String> globalFilter;
    private final String applicationId;

    public GenericBigQueryDataSourceReader(TableInfo table, ReadSessionCreatorConfig readSessionCreatorConfig,
                                           BigQueryClient bigQueryClient, BigQueryReadClientFactory bigQueryReadClientFactory,
                                           BigQueryTracerFactory bigQueryTracerFactory,
                                           Optional<String> globalFilter, String applicationId) {
        this.table = table;
        this.tableId = table.getTableId();
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        this.bigQueryClient = bigQueryClient;
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.bigQueryTracerFactory = bigQueryTracerFactory;
        this.applicationId = applicationId;
        this.readSessionCreator = new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);
        this.globalFilter = globalFilter;
    }


    public TableInfo getTable() {
        return table;
    }

    public TableId getTableId() {
        return tableId;
    }

    public ReadSessionCreatorConfig getReadSessionCreatorConfig() {
        return readSessionCreatorConfig;
    }

    public BigQueryClient getBigQueryClient() {
        return bigQueryClient;
    }

    public BigQueryReadClientFactory getBigQueryReadClientFactory() {
        return bigQueryReadClientFactory;
    }

    public BigQueryTracerFactory getBigQueryTracerFactory() {
        return bigQueryTracerFactory;
    }

    public ReadSessionCreator getReadSessionCreator() {
        return readSessionCreator;
    }

    public Optional<String> getGlobalFilter() {
        return globalFilter;
    }

    public String getApplicationId() {
        return applicationId;
    }


}
