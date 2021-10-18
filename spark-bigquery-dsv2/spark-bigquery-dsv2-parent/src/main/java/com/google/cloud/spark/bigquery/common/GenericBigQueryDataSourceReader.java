package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;
import java.util.OptionalLong;

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



    public GenericBigQueryDataSourceReader(TableInfo table, TableId tableId, ReadSessionCreatorConfig readSessionCreatorConfig,
                                           BigQueryClient bigQueryClient, BigQueryReadClientFactory bigQueryReadClientFactory,
                                           BigQueryTracerFactory bigQueryTracerFactory, ReadSessionCreator readSessionCreator,
                                           Optional<String> globalFilter, String applicationId) {
        this.table = table;
        this.tableId = tableId;
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        this.bigQueryClient = bigQueryClient;
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.bigQueryTracerFactory = bigQueryTracerFactory;
        this.applicationId = applicationId;
        this.readSessionCreator = new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);
        this.globalFilter = globalFilter;
    }


}
