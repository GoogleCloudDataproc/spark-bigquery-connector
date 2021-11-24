/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceReader;
import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryBatchScan implements Scan, Batch, SupportsReportStatistics {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryBatchScan.class);
  private static Statistics UNKNOWN_STATISTICS =
      new Statistics() {

        @Override
        public OptionalLong sizeInBytes() {
          return OptionalLong.empty();
        }

        @Override
        public OptionalLong numRows() {
          return OptionalLong.empty();
        }
      };
  private GenericBigQueryDataSourceReader dataSourceReader;

  public BigQueryBatchScan(
      TableInfo table,
      TableId tableId,
      Optional<StructType> schema,
      Optional<StructType> userProvidedSchema,
      Map<String, StructField> fields,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory bigQueryTracerFactory,
      ReadSessionCreator readSessionCreator,
      Optional<String> globalFilter,
      Filter[] pushedFilters,
      String applicationId) {
    this.dataSourceReader =
        new GenericBigQueryDataSourceReader(
            table,
            tableId,
            schema,
            userProvidedSchema,
            fields,
            readSessionCreatorConfig,
            bigQueryClient,
            bigQueryReadClientFactory,
            bigQueryTracerFactory,
            readSessionCreator,
            globalFilter,
            pushedFilters,
            applicationId);
  }

  @Override
  public InputPartition[] planInputPartitions() {
    if (this.dataSourceReader.isEmptySchema()) {
      return createEmptyProjectionPartitions();
    }
    List<ReadStream> streamList;
    if (this.dataSourceReader.enableBatchRead()) {
      this.dataSourceReader.createReadSession(true);
      streamList = this.dataSourceReader.getReadSession().getStreamsList();
      if (this.dataSourceReader.getSelectedFields().isEmpty()) {
        this.dataSourceReader.emptySchemaForPartition();
      }
      InputPartition[] arrowInputPartition = new InputPartition[streamList.size()];
      logger.info(
          "Created read session for {}: {} for application id: {}",
          this.dataSourceReader.getTableId().toString(),
          this.dataSourceReader.getReadSession().getName(),
          this.dataSourceReader.getApplicationId());
      for (int i = 0; i < streamList.size(); i++) {
        arrowInputPartition[i] =
            new ArrowInputPartition(
                this.dataSourceReader.getBigQueryReadClientFactory(),
                this.dataSourceReader.getBigQueryTracerFactory(),
                streamList.get(i).getName(),
                this.dataSourceReader.getReadSessionCreatorConfig().toReadRowsHelperOptions(),
                this.dataSourceReader.getSelectedFields(),
                this.dataSourceReader.getReadSessionResponse(),
                this.dataSourceReader.getUserProvidedSchema());
      }
      return arrowInputPartition;
    }
    this.dataSourceReader.createReadSession(false);
    streamList = this.dataSourceReader.getReadSession().getStreamsList();
    logger.info(
        "Created read session for {}: {} for application id: {}",
        this.dataSourceReader.getTableId().toString(),
        this.dataSourceReader.getReadSession().getName(),
        this.dataSourceReader.getApplicationId());
    InputPartition[] bigQueryInputPartitions = new InputPartition[streamList.size()];
    for (int i = 0; i < streamList.size(); i++) {
      bigQueryInputPartitions[i] =
          new BigQueryInputPartition(
              this.dataSourceReader.getBigQueryReadClientFactory(),
              streamList.get(i).getName(),
              this.dataSourceReader.getReadSessionCreatorConfig().toReadRowsHelperOptions(),
              this.dataSourceReader.createConverter());
    }
    return bigQueryInputPartitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BigQueryPartitionReaderFactory();
  }

  public InputPartition[] createEmptyProjectionPartitions() {
    this.dataSourceReader.createEmptyProjectionPartitions();
    InputPartition[] partitions =
        IntStream.range(0, this.dataSourceReader.getPartitionsCount())
            .mapToObj(
                ignored ->
                    new BigQueryEmptyProjectInputPartition(
                        this.dataSourceReader.getPartitionSize()))
            .toArray(BigQueryEmptyProjectInputPartition[]::new);
    partitions[0] =
        new BigQueryEmptyProjectInputPartition(this.dataSourceReader.getFirstPartitionSize());
    return partitions;
  }

  @Override
  public StructType readSchema() {
    return this.dataSourceReader.readSchema();
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public Statistics estimateStatistics() {
    return this.dataSourceReader.getTable().getDefinition().getType() == TableDefinition.Type.TABLE
        ? new StandardTableStatistics(this.dataSourceReader.getTable().getDefinition())
        : UNKNOWN_STATISTICS;
  }

  public ImmutableList<String> emptySchemaForPartition(
      ImmutableList<String> selectedFields, ReadSessionResponse readSessionResponse) {
    // means select *
    Schema tableSchema =
        SchemaConverters.getSchemaWithPseudoColumns(readSessionResponse.getReadTableInfo());
    return tableSchema.getFields().stream()
        .map(Field::getName)
        .collect(ImmutableList.toImmutableList());
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
