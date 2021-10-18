package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class BigQueryScan
    implements Scan,
        Batch,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsReportStatistics {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryScan.class);
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

  private final TableInfo table;
  private final TableId tableId;
  private final ReadSessionCreatorConfig readSessionCreatorConfig;
  private final BigQueryClient bigQueryClient;
  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory bigQueryTracerFactory;
  private final ReadSessionCreator readSessionCreator;
  private final Optional<String> globalFilter;
  private final String applicationId;
  private Optional<StructType> schema;
  private Optional<StructType> userProvidedSchema;
  private Filter[] pushedFilters = new Filter[] {};
  private Map<String, StructField> fields;

  public BigQueryScan(
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Optional<String> globalFilter,
      Optional<StructType> schema,
      String applicationId) {
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
  }

  private boolean isEmptySchema() {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return null;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return null;
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

  List<InputPartition> createEmptyProjectionPartitions() {
    long rowCount = bigQueryClient.calculateTableSize(tableId, globalFilter);
    logger.info("Used optimized BQ count(*) path. Count: " + rowCount);
    int partitionsCount = readSessionCreatorConfig.getDefaultParallelism();
    int partitionSize = (int) (rowCount / partitionsCount);
    /*InputPartition[] partitions = IntStream.range(0, partitionsCount)
            .mapToObj(ignored -> new BigQueryEmptyProjectionInputPartition(partitionSize))
            .toArray(BigQueryEmptyProjectionInputPartition[]::new);
    int firstPartitionSize = partitionSize + (int) (rowCount % partitionsCount);
    partitions[0] = new BigQueryEmptyProjectionInputPartition(firstPartitionSize);*/
    return null;
  }

  @Override
  public StructType readSchema() {
    return null;
  }

  @Override
  public Statistics estimateStatistics() {
    return table.getDefinition().getType() == TableDefinition.Type.TABLE
        ? new StandardTableStatistics(table.getDefinition())
        : UNKNOWN_STATISTICS;
  }

  @Override
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
  public Scan build() {
    return this;
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
