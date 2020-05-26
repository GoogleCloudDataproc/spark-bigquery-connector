/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BigQueryDataSourceReader implements
        DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsReportStatistics {

    private static Statistics UNKNOWN_STATISTICS = new Statistics() {

        @Override
        public OptionalLong sizeInBytes() {
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong numRows() {
            return OptionalLong.empty();
        }
    };

    private final TableInfo table;
    private final TableId tableId;
    private final ReadSessionCreatorConfig readSessionCreatorConfig;
    private final BigQueryClient bigQueryClient;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final ReadSessionCreator readSessionCreator;
    private final Optional<String> globalFilter;
    private Optional<StructType> schema;
    private Filter[] pushedFilters = new Filter[]{};

    public BigQueryDataSourceReader(
            TableInfo table,
            BigQueryClient bigQueryClient,
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            ReadSessionCreatorConfig readSessionCreatorConfig,
            Optional<String> globalFilter,
            Optional<StructType> schema) {
        this.table = table;
        this.tableId = table.getTableId();
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        this.bigQueryClient = bigQueryClient;
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
        this.readSessionCreator = new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryStorageClientFactory);
        this.globalFilter = globalFilter;
        this.schema = schema;
    }

    @Override
    public StructType readSchema() {
        // TODO: rely on Java code
        return schema.orElse(SchemaConverters.toSpark(table.getDefinition().getSchema()));
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        if (schema.map(StructType::isEmpty).orElse(false)) {
            // create empty projection
            return createEmptyProjectionPartitions();
        }

        ImmutableList<String> selectedFields = schema
                .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
                .orElse(ImmutableList.of());
        Optional<String> filter = emptyIfNeeded(SparkFilterUtils.getCompiledFilter(
                readSessionCreatorConfig.getReadDataFormat(), globalFilter, pushedFilters));
        ReadSessionResponse readSessionResponse = readSessionCreator.create(
                tableId, selectedFields, filter, readSessionCreatorConfig.getMaxParallelism());
        Storage.ReadSession readSession = readSessionResponse.getReadSession();
        return readSession.getStreamsList().stream()
                .map(stream -> new BigQueryInputPartition(
                        bigQueryStorageClientFactory,
                        stream.getName(),
                        readSessionCreatorConfig.getMaxReadRowsRetries(),
                        createConverter(selectedFields, readSessionResponse)))
                .collect(Collectors.toList());
    }

    private ReadRowsResponseToInternalRowIteratorConverter createConverter(
            ImmutableList<String> selectedFields, ReadSessionResponse readSessionResponse) {
        ReadRowsResponseToInternalRowIteratorConverter converter;
        if (readSessionCreatorConfig.getReadDataFormat() == Storage.DataFormat.AVRO) {
            Schema schema = readSessionResponse.getReadTableInfo().getDefinition().getSchema();
            if (selectedFields.isEmpty()) {
                // means select *
                selectedFields = schema.getFields().stream()
                        .map(Field::getName)
                        .collect(ImmutableList.toImmutableList());
            } else {
                Set<String> requiredColumnSet = ImmutableSet.copyOf(selectedFields);
                schema = Schema.of(schema.getFields().stream()
                        .filter(field -> requiredColumnSet.contains(field.getName()))
                        .collect(Collectors.toList()));
            }
            return ReadRowsResponseToInternalRowIteratorConverter.avro(
                    schema,
                    selectedFields,
                    readSessionResponse.getReadSession().getAvroSchema().getSchema());
        } else {
            return ReadRowsResponseToInternalRowIteratorConverter.arrow(
                    selectedFields,
                    readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema());
        }
    }

    List<InputPartition<InternalRow>> createEmptyProjectionPartitions() {
        long rowCount = bigQueryClient.calculateTableSize(tableId, globalFilter);
        int partitionsCount = readSessionCreatorConfig.getDefaultParallelism();
        int partitionSize = (int) (rowCount / partitionsCount);
        InputPartition<InternalRow>[] partitions = IntStream
                .range(0, partitionsCount)
                .mapToObj(ignored -> new BigQueryEmptyProjectionInputPartition(partitionSize))
                .toArray(BigQueryEmptyProjectionInputPartition[]::new);
        int firstPartitionSize = partitionSize + (int) (rowCount % partitionsCount);
        partitions[0] = new BigQueryEmptyProjectionInputPartition(firstPartitionSize);
        return ImmutableList.copyOf(partitions);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> handledFilters = new ArrayList<>();
        List<Filter> unhandledFilters = new ArrayList<>();
        for (Filter filter : filters) {
            if (SparkFilterUtils.isHandled(filter, readSessionCreatorConfig.getReadDataFormat())) {
                handledFilters.add(filter);
            } else {
                unhandledFilters.add(filter);
            }
        }
        pushedFilters = handledFilters.stream().toArray(Filter[]::new);
        return unhandledFilters.stream().toArray(Filter[]::new);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = Optional.ofNullable(requiredSchema);
    }

    Optional<String> emptyIfNeeded(String value) {
        return (value == null || value.length() == 0) ?
                Optional.empty() : Optional.of(value);
    }


    @Override
    public Statistics estimateStatistics() {
        return table.getDefinition().getType() == TableDefinition.Type.TABLE ?
                new StandardTableStatistics(table.getDefinition()) :
                UNKNOWN_STATISTICS;

    }
}

class StandardTableStatistics implements Statistics {

    private StandardTableDefinition tableDefinition;

    public StandardTableStatistics(StandardTableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
    }

    @Override
    public OptionalLong sizeInBytes() {
        return OptionalLong.of(tableDefinition.getNumBytes());
    }

    @Override
    public OptionalLong numRows() {
        return OptionalLong.of(tableDefinition.getNumRows());
    }
}
