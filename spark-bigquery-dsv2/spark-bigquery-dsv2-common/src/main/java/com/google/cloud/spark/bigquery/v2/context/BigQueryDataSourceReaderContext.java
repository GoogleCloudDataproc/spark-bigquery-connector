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
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
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
  private final SparkBigQueryConfig options;
  private final SQLContext sqlContext;
  private final BigQueryRDDFactory bigQueryRDDFactory;
  private final Optional<String> globalFilter;
  private final String applicationId;
  private Optional<StructType> schema;
  private Optional<StructType> userProvidedSchema;
  private Filter[] pushedFilters = new Filter[] {};
  private Filter[] allFilters = new Filter[] {};
  private Map<String, StructField> fields;
  private Optional<ImmutableList<String>> selectedFields = Optional.empty();
  private List<ArrowInputPartitionContext> plannedInputPartitionContexts;
  // Lazy loading using Supplier will ensure that createReadSession is called only once and
  // readSessionResponse is cached.
  // Purpose is to create read session either in estimateStatistics or planInputPartitionContexts,
  // whichever is called first.
  // In Spark 3.1 connector, "estimateStatistics" is called before
  // "planBatchInputPartitionContexts" or
  // "planInputPartitionContexts". We will use this to get table statistics in estimateStatistics.
  private Supplier<ReadSessionResponse> readSessionResponse;

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
    for (StructField field : convertedSchema.fields()) {
      fields.put(field.name(), field);
    }
    this.applicationId = applicationId;
    this.options = options;
    this.sqlContext = sqlContext;
    this.bigQueryRDDFactory =
        new BigQueryRDDFactory(
            bigQueryClient, bigQueryReadClientFactory, bigQueryTracerFactory, options, sqlContext);
    resetReadSessionResponse();
  }

  private void resetReadSessionResponse() {
    this.readSessionResponse = Suppliers.memoize(this::createReadSession);
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

    ReadSession readSession = readSessionResponse.get().getReadSession();

    return readSession.getStreamsList().stream()
        .map(
            stream ->
                new BigQueryInputPartitionContext(
                    bigQueryReadClientFactory,
                    stream.getName(),
                    readSessionCreatorConfig.toReadRowsHelperOptions(),
                    createConverter(
                        selectedFields.get(), readSessionResponse.get(), userProvidedSchema)));
  }

  public Optional<String> getCombinedFilter() {
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

    ReadSession readSession = readSessionResponse.get().getReadSession();

    ImmutableList<String> tempSelectedFields = selectedFields.get();
    if (tempSelectedFields.isEmpty()) {
      // means select *
      Schema tableSchema =
          SchemaConverters.getSchemaWithPseudoColumns(readSessionResponse.get().getReadTableInfo());
      tempSelectedFields =
          tableSchema.getFields().stream()
              .map(Field::getName)
              .collect(ImmutableList.toImmutableList());
    }
    ImmutableList<String> partitionSelectedFields = tempSelectedFields;
    Optional<StructType> arrowSchema = Optional.of(userProvidedSchema.orElse(readSchema()));
    plannedInputPartitionContexts =
        Streams.stream(
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
                        readSessionResponse.get(),
                        arrowSchema))
            .collect(Collectors.toList());
    return plannedInputPartitionContexts.stream()
        .map(ctx -> (InputPartitionContext<ColumnarBatch>) ctx);
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
          userProvidedSchema,
          Optional.empty() /* bigQueryStorageReadRowTracer */);
    }
    throw new IllegalArgumentException(
        "No known converted for " + readSessionCreatorConfig.getReadDataFormat());
  }

  private ReadSessionResponse createReadSession() {
    selectedFields =
        Optional.of(
            schema
                .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
                .orElse(ImmutableList.copyOf(fields.keySet())));
    Optional<String> filter = getCombinedFilter();
    ReadSessionResponse response = readSessionCreator.create(tableId, selectedFields.get(), filter);
    logger.info(
        "Created read session for {}: {} for application id: {}",
        tableId.toString(),
        response.getReadSession().getName(),
        applicationId);
    return response;
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

    allFilters = filters;
    pushedFilters = handledFilters.stream().toArray(Filter[]::new);
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  public Filter[] getAllFilters() {
    return allFilters;
  }

  public void filter(Filter[] filters) {
    logger.info(String.format("Use Dynamic Partition Pruning runtime filters: %s", filters));
    if (plannedInputPartitionContexts == null) {
      logger.error("Should have planned partitions.");
      return;
    }

    ImmutableList<Filter> newFilters =
        SparkBigQueryUtil.extractPartitionAndClusteringFilters(
            table, ImmutableList.copyOf(filters));
    if (newFilters.isEmpty()) {
      // no partitioning and no clustering, this is probably a dimension table.
      // It means the filter combined filter won't change, so no need to create another read session
      // we are done here.
      logger.info(
          "Could not find filters for partition of clustering field for table {}, aborting DPP filter",
          BigQueryUtil.friendlyTableName(tableId));
      return;
    }
    pushedFilters =
        Stream.concat(Arrays.stream(pushedFilters), newFilters.stream()).toArray(Filter[]::new);
    Optional<String> combinedFilter = getCombinedFilter();
    if (!BigQueryUtil.filterLengthInLimit(combinedFilter)) {
      logger.warn(
          "New filter for Dynamic Partition Pruning is too large, skipping partition pruning");
      return;
    }

    // Copies previous planned input partition contexts.
    List<ArrowInputPartitionContext> previousInputPartitionContexts = plannedInputPartitionContexts;
    resetReadSessionResponse();
    // Creates a new read session, this creates a new plannedInputPartitionContexts.
    planBatchInputPartitionContexts();

    if (plannedInputPartitionContexts.size() > previousInputPartitionContexts.size()) {
      logger.error(
          String.format(
              "New partitions should not be more than originally planned. Previously had %d streams, now has %d.",
              previousInputPartitionContexts.size(), plannedInputPartitionContexts.size()));
      return;
    }
    logger.info(
        String.format(
            "Use Dynamic Partition Pruning, originally planned %d, adjust to %d partitions",
            previousInputPartitionContexts.size(), plannedInputPartitionContexts.size()));

    // TODO: Spread streams more evenly. This solution reduces the parallelism as it potentially
    // leaves partitions without streams while other may have more than one stream.

    // first let's update the streams in the previous planned partitions
    for (int i = 0; i < plannedInputPartitionContexts.size(); i++) {
      previousInputPartitionContexts
          .get(i)
          .resetStreamNamesFrom(plannedInputPartitionContexts.get(i));
    }
    // second, clear the redundant partitions
    for (int i = plannedInputPartitionContexts.size();
        i < previousInputPartitionContexts.size();
        i++) {
      previousInputPartitionContexts.get(i).clearStreamsList();
    }
  }

  public void pruneColumns(StructType requiredSchema) {
    this.schema = Optional.ofNullable(requiredSchema);
  }

  public StatisticsContext estimateStatistics() {
    if (table.getDefinition().getType() == TableDefinition.Type.TABLE) {
      // Create StatisticsContext with infromation from read session response
      final long tableSizeInBytes =
          readSessionResponse.get().getReadSession().getEstimatedTotalBytesScanned();
      final long numRowsInTable = readSessionResponse.get().getReadSession().getEstimatedRowCount();

      StatisticsContext tableStatisticsContext =
          new StatisticsContext() {
            @Override
            public OptionalLong sizeInBytes() {
              return OptionalLong.of(tableSizeInBytes);
            }

            @Override
            public OptionalLong numRows() {
              return OptionalLong.of(numRowsInTable);
            }
          };

      return tableStatisticsContext;
    } else {
      return UNKNOWN_STATISTICS;
    }
  }

  public String getTableName() {
    return tableId.getTable();
  }

  public String getFullTableName() {
    return BigQueryUtil.friendlyTableName(tableId);
  }

  public TableId getTableId() {
    return tableId;
  }

  public BigQueryRDDFactory getBigQueryRddFactory() {
    return this.bigQueryRDDFactory;
  }

  public TableInfo getTableInfo() {
    return this.table;
  }
}
