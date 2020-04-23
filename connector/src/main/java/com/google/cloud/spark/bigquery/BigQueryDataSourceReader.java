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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryStorageClientFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Optional;

public class BigQueryDataSourceReader implements
        DataSourceReader, SupportsPushDownRequiredColumns, SupportsPushDownFilters {

    private final TableId tableId;
    private final ReadSessionCreatorConfig readSessionCreatorConfig;
    private final BigQueryClient bigQueryClient;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final ReadSessionCreator readSessionCreator;
    private final StructType schema;

    private Storage.ReadSession readSession;
    private StructType requiredSchema = new StructType();

    public BigQueryDataSourceReader(
            TableId tableId,
            BigQueryClient bigQueryClient,
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            ReadSessionCreatorConfig readSessionCreatorConfig,
            StructType schema) {
        this.tableId = tableId;
        this.readSessionCreatorConfig = readSessionCreatorConfig;
        this.bigQueryClient = bigQueryClient;
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
        this.readSessionCreator = new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryStorageClientFactory);
        this.schema = schema;
    }

    @Override
    public StructType readSchema() {
        createReadSessionIfNeeded();
        return requiredSchema != null ? requiredSchema : schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        createReadSessionIfNeeded();
        return null;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        return new Filter[0];
    }

    @Override
    public Filter[] pushedFilters() {
        return new Filter[0];
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    void createReadSessionIfNeeded() {
        if (readSession == null && !requiredSchema.isEmpty()) {
            createReadSession();
        }
    }

    void createReadSession() {
        // TODO
        ImmutableList<String> selectedFields = ImmutableList.copyOf(schema.fieldNames());
        Optional<String> filter = null;
        readSession = readSessionCreator.create(
                tableId, selectedFields, filter, readSessionCreatorConfig.getMaxParallelism());
    }


}
