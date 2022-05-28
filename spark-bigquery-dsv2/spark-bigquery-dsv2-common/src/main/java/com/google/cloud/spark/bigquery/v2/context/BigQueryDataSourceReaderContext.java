/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2.context;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;
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
import java.util.stream.Stream;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class BigQueryDataSourceReaderContext {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryDataSourceReaderContext.class);

  private static StatisticsContext UNKNOWN_STATISTICS =
      new StatisticsContext() {

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
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory bigQueryTracerFactory;
  private final ReadSessionCreator readSessionCreator;
  private final Optional<String> globalFilter;
  private final String applicationId;
  private final SparkBigQueryConfig options;
  private final SQLContext sqlContext;
  private final BigQueryRDDFactory bigQueryRDDFactory;
  private Optional<StructType> schema;
  private Optional<StructType> userProvidedSchema;
  private Filter[] pushedFilters = new Filter[] {};
  private Map<String, StructField> fields;

  public BigQueryDataSourceReaderContext(
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Optional<String> globalFilter,
      Optional<StructType> schema,
      String applicationId,
      SparkBigQueryConfig options,
      SQLContext sqlContext) {
    this.table = table;
    this.tableId = table.getTableId();
    this.readSessionCreatorConfig = readSessionCreatorConfig;
    this.bigQueryClient = bigQueryClient;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.bigQueryTracerFactory = tracerFactory;
    this.readSessionCreator =
        new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);
    this.globalFilter = globalFilter;
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
    this.applicationId = applicationId;
    this.options = options;
    this.sqlContext = sqlContext;
    this.bigQueryRDDFactory =
        new BigQueryRDDFactory(bigQueryClient, bigQueryReadClientFactory, options, sqlContext);
  }

  public StructType readSchema() {
    // TODO: rely on Java code
    return schema.orElse(
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table)));
  }

  public boolean enableBatchRead() {
    return readSessionCreatorConfig.getReadDataFormat() == DataFormat.ARROW && !isEmptySchema();
  }

  public Stream<InputPartitionContext<InternalRow>> planInputPartitionContexts() {
    if (isEmptySchema()) {
      // create empty projection
      return createEmptyProjectionPartitions();
    }

    ImmutableList<String> selectedFields =
        schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.copyOf(fields.keySet()));
    Optional<String> filter = getCombinedFilter();
    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(tableId, selectedFields, filter);
    ReadSession readSession = readSessionResponse.getReadSession();
    logger.info(
        "Created read session for {}: {} for application id: {}",
        tableId.toString(),
        readSession.getName(),
        applicationId);
    return readSession.getStreamsList().stream()
        .map(
            stream ->
                new BigQueryInputPartitionContext(
                    bigQueryReadClientFactory,
                    stream.getName(),
                    readSessionCreatorConfig.toReadRowsHelperOptions(),
                    createConverter(selectedFields, readSessionResponse, userProvidedSchema)));
  }

  private Optional<String> getCombinedFilter() {
    return BigQueryUtil.emptyIfNeeded(
        SparkFilterUtils.getCompiledFilter(
            readSessionCreatorConfig.getPushAllFilters(),
            readSessionCreatorConfig.getReadDataFormat(),
            globalFilter,
            pushedFilters));
  }

  public Stream<InputPartitionContext<ColumnarBatch>> planBatchInputPartitionContexts() {
    if (!enableBatchRead()) {
      throw new IllegalStateException("Batch reads should not be enabled");
    }
    ImmutableList<String> selectedFields =
        schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.copyOf(fields.keySet()));
    Optional<String> filter = getCombinedFilter();
    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(tableId, selectedFields, filter);
    ReadSession readSession = readSessionResponse.getReadSession();
    logger.info(
        "Created read session for {}: {} for application id: {}",
        tableId.toString(),
        readSession.getName(),
        applicationId);

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
                readSession.getStreamsList(), readSessionCreatorConfig.streamsPerPartition()))
        .map(
            streams ->
                new ArrowInputPartitionContext(
                    bigQueryReadClientFactory,
                    bigQueryTracerFactory,
                    streams.stream()
                        .map(ReadStream::getName)
                        // This formulation is used to guarantee a serializable list.
                        .collect(Collectors.toCollection(ArrayList::new)),
                    readSessionCreatorConfig.toReadRowsHelperOptions(),
                    partitionSelectedFields,
                    readSessionResponse,
                    userProvidedSchema));
  }

  private boolean isEmptySchema() {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  private ReadRowsResponseToInternalRowIteratorConverter createConverter(
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    ReadRowsResponseToInternalRowIteratorConverter converter;
    DataFormat format = readSessionCreatorConfig.getReadDataFormat();
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
        "No known converted for " + readSessionCreatorConfig.getReadDataFormat());
  }

  Stream<InputPartitionContext<InternalRow>> createEmptyProjectionPartitions() {
    Optional<String> filter = getCombinedFilter();
    long rowCount = bigQueryClient.calculateTableSize(tableId, filter);
    logger.info("Used optimized BQ count(*) path. Count: " + rowCount);
    int partitionsCount = readSessionCreatorConfig.getDefaultParallelism();
    int partitionSize = (int) (rowCount / partitionsCount);
    InputPartitionContext<InternalRow>[] partitions =
        IntStream.range(0, partitionsCount)
            .mapToObj(ignored -> new EmptyProjectionInputPartitionContext(partitionSize))
            .toArray(EmptyProjectionInputPartitionContext[]::new);
    int firstPartitionSize = partitionSize + (int) (rowCount % partitionsCount);
    partitions[0] = new EmptyProjectionInputPartitionContext(firstPartitionSize);
    return Stream.of(partitions);
  }

  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> handledFilters = new ArrayList<>();
    List<Filter> unhandledFilters = new ArrayList<>();
    for (Filter filter : filters) {
      if (SparkFilterUtils.isTopLevelFieldHandled(
          readSessionCreatorConfig.getPushAllFilters(),
          filter,
          readSessionCreatorConfig.getReadDataFormat(),
          fields)) {
        handledFilters.add(filter);
      } else {
        unhandledFilters.add(filter);
      }
    }
    pushedFilters = handledFilters.stream().toArray(Filter[]::new);
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  public void pruneColumns(StructType requiredSchema) {
    this.schema = Optional.ofNullable(requiredSchema);
  }

  public StatisticsContext estimateStatistics() {
    return table.getDefinition().getType() == TableDefinition.Type.TABLE
        ? new StandardTableStatisticsContext(table.getDefinition())
        : UNKNOWN_STATISTICS;
  }

  public String getTableName() {
    return tableId.getTable();
  }

  public String getFullTableName() {
    return BigQueryUtil.friendlyTableName(tableId);
  }

  public BigQueryRDDFactory getBigQueryRddFactory() {
    return this.bigQueryRDDFactory;
  }
}
