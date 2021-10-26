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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class BigQueryDataSourceReader extends GenericBigQueryDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsReportStatistics,
        SupportsScanColumnarBatch {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceReader.class);

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

  private Optional<StructType> schema;
  private Optional<StructType> userProvidedSchema;
  private Filter[] pushedFilters = new Filter[] {};
  private Map<String, StructField> fields;

  public BigQueryDataSourceReader(
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Optional<String> globalFilter,
      Optional<StructType> schema,
      String applicationId) {
    super(
        table,
        readSessionCreatorConfig,
        bigQueryClient,
        bigQueryReadClientFactory,
        tracerFactory,
        globalFilter,
        applicationId);

    StructType convertedSchema =
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
    if (schema.isPresent()) {
      this.schema = schema;
      this.userProvidedSchema = schema;
    } else {
      this.schema = Optional.of(convertedSchema);
      this.userProvidedSchema = Optional.empty();
    }
    // We want to keep the key order
    this.fields = new LinkedHashMap<>();
    for (StructField field : JavaConversions.seqAsJavaList(convertedSchema)) {
      fields.put(field.name(), field);
    }
  }

  @Override
  public StructType readSchema() {

    // TODO: rely on Java code
    return schema.orElse(
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(super.getTable())));
  }

  @Override
  public boolean enableBatchRead() {
    return super.getReadSessionCreatorConfig().getReadDataFormat() == DataFormat.ARROW
        && !isEmptySchema();
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    if (isEmptySchema()) {
      // create empty projection
      return createEmptyProjectionPartitions();
    }

    ImmutableList<String> selectedFields =
        schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.of());
    Optional<String> filter = getCombinedFilter();
    ReadSessionResponse readSessionResponse =
        super.getReadSessionCreator().create(super.getTableId(), selectedFields, filter);
    ReadSession readSession = readSessionResponse.getReadSession();
    logger.info(
        "Created read session for {}: {} for application id: {}",
        super.getTableId().toString(),
        readSession.getName(),
        super.getApplicationId());
    return readSession.getStreamsList().stream()
        .map(
            stream ->
                new BigQueryInputPartition(
                    super.getBigQueryReadClientFactory(),
                    stream.getName(),
                    super.getReadSessionCreatorConfig().toReadRowsHelperOptions(),
                    createConverter(selectedFields, readSessionResponse, userProvidedSchema)))
        .collect(Collectors.toList());
  }

  private Optional<String> getCombinedFilter() {
    return emptyIfNeeded(
        SparkFilterUtils.getCompiledFilter(
            super.getReadSessionCreatorConfig().getPushAllFilters(),
            super.getReadSessionCreatorConfig().getReadDataFormat(),
            super.getGlobalFilter(),
            pushedFilters));
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    if (!enableBatchRead()) {
      throw new IllegalStateException("Batch reads should not be enabled");
    }
    ImmutableList<String> selectedFields =
        schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.copyOf(fields.keySet()));
    Optional<String> filter = getCombinedFilter();
    ReadSessionResponse readSessionResponse =
        super.getReadSessionCreator().create(super.getTableId(), selectedFields, filter);
    ReadSession readSession = readSessionResponse.getReadSession();
    logger.info(
        "Created read session for {}: {} for application id: {}",
        super.getTableId().toString(),
        readSession.getName(),
        super.getApplicationId());

    if (selectedFields.isEmpty()) {
      // means select *
      Schema tableSchema =
          SchemaConverters.getSchemaWithPseudoColumns(readSessionResponse.getReadTableInfo());
      selectedFields =
          tableSchema.getFields().stream()
              .map(Field::getName)
              .collect(ImmutableList.toImmutableList());
    }

    ImmutableList<String> partitionSelectedFields = selectedFields;
    return Streams.stream(
            Iterables.partition(
                readSession.getStreamsList(),
                super.getReadSessionCreatorConfig().streamsPerPartition()))
        .map(
            streams ->
                new ArrowInputPartition(
                    super.getBigQueryReadClientFactory(),
                    super.getBigQueryTracerFactory(),
                    streams.stream()
                        .map(ReadStream::getName)
                        // This formulation is used to guarantee a serializable list.
                        .collect(Collectors.toCollection(ArrayList::new)),
                    super.getReadSessionCreatorConfig().toReadRowsHelperOptions(),
                    partitionSelectedFields,
                    readSessionResponse,
                    userProvidedSchema))
        .collect(Collectors.toList());
  }

  private boolean isEmptySchema() {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  // this method should move into the spark bigquery connector common
  private ReadRowsResponseToInternalRowIteratorConverter createConverter(
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    ReadRowsResponseToInternalRowIteratorConverter converter;
    DataFormat format = super.getReadSessionCreatorConfig().getReadDataFormat();
    if (format == DataFormat.AVRO) {
      Schema schema =
          SchemaConverters.getSchemaWithPseudoColumns(readSessionResponse.getReadTableInfo());
      if (selectedFields.isEmpty()) {
        // means select *
        selectedFields =
            schema.getFields().stream()
                .map(Field::getName)
                .collect(ImmutableList.toImmutableList());
      } else {
        Set<String> requiredColumnSet = ImmutableSet.copyOf(selectedFields);
        schema =
            Schema.of(
                schema.getFields().stream()
                    .filter(field -> requiredColumnSet.contains(field.getName()))
                    .collect(Collectors.toList()));
      }
      return ReadRowsResponseToInternalRowIteratorConverter.avro(
          schema,
          selectedFields,
          readSessionResponse.getReadSession().getAvroSchema().getSchema(),
          userProvidedSchema);
    }
    throw new IllegalArgumentException(
        "No known converted for " + super.getReadSessionCreatorConfig().getReadDataFormat());
  }

  List<InputPartition<InternalRow>> createEmptyProjectionPartitions() {
    Optional<String> filter = getCombinedFilter();
    long rowCount = super.getBigQueryClient().calculateTableSize(super.getTableId(), filter);
    logger.info("Used optimized BQ count(*) path. Count: " + rowCount);
    int partitionsCount = super.getReadSessionCreatorConfig().getDefaultParallelism();
    int partitionSize = (int) (rowCount / partitionsCount);
    InputPartition<InternalRow>[] partitions =
        IntStream.range(0, partitionsCount)
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
      if (SparkFilterUtils.isTopLevelFieldHandled(
          super.getReadSessionCreatorConfig().getPushAllFilters(),
          filter,
          super.getReadSessionCreatorConfig().getReadDataFormat(),
          fields)) {
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
    return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
  }

  @Override
  public Statistics estimateStatistics() {
    return super.getTable().getDefinition().getType() == TableDefinition.Type.TABLE
        ? new StandardTableStatistics(super.getTable().getDefinition())
        : UNKNOWN_STATISTICS;
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
