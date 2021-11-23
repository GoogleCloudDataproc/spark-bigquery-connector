package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

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
  private final TableInfo table;
  private final TableId tableId;
  private final ReadSessionCreatorConfig readSessionCreatorConfig;
  private final BigQueryClient bigQueryClient;
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory bigQueryTracerFactory;
  private final ReadSessionCreator readSessionCreator;
  private final Optional<String> globalFilter;
  private final String applicationId;
  private Optional<StructType> schema;
  private Optional<StructType> userProvidedSchema;
  private Filter[] pushedFilters = new Filter[] {};
  private Map<String, StructField> fields;
  //  private GenericBigQuerySchemaHelper schemaHelper;

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
    this.table = table;
    this.tableId = tableId;
    this.readSessionCreatorConfig = readSessionCreatorConfig;
    this.bigQueryClient = bigQueryClient;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.bigQueryTracerFactory = bigQueryTracerFactory;
    this.applicationId = applicationId;
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
      this.fields.put(field.name(), field);
    }
    this.pushedFilters = pushedFilters;
  }

  public boolean isEmptySchema(Optional<StructType> schema) {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  public boolean isEnableBatchRead(
      ReadSessionCreatorConfig readSessionCreatorConfig, Optional<StructType> schema) {
    return readSessionCreatorConfig.getReadDataFormat() == DataFormat.ARROW
        && !isEmptySchema(schema);
  }

  @Override
  public InputPartition[] planInputPartitions() {
    if (isEmptySchema(schema)) {
      return createEmptyProjectionPartitions();
    }
    ImmutableList<String> selectedFields;
    Optional<String> filter;
    ReadSessionResponse readSessionResponse;
    ReadSession readSession;
    //    logger.info(
    //        "Created read session for {}: {} for application id: {}",
    //        tableId.toString(),
    //        readSession.getName(),
    //        applicationId);
    List<ReadStream> streamList;
    if (isEnableBatchRead(readSessionCreatorConfig, schema)) {
      selectedFields =
          schema
              .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
              .orElse(ImmutableList.copyOf(fields.keySet()));
      filter = getCombinedFilter();
      readSessionResponse = readSessionCreator.create(tableId, selectedFields, filter);
      readSession = readSessionResponse.getReadSession();
      streamList = readSession.getStreamsList();
      if (selectedFields.isEmpty()) {
        selectedFields = emptySchemaForPartition(selectedFields, readSessionResponse);
      }
      InputPartition[] arrowInputPartition = new InputPartition[streamList.size()];
      for (int i = 0; i < streamList.size(); i++) {
        arrowInputPartition[i] =
            new ArrowInputPartition(
                bigQueryReadClientFactory,
                bigQueryTracerFactory,
                streamList.get(i).getName(),
                readSessionCreatorConfig.toReadRowsHelperOptions(),
                selectedFields,
                readSessionResponse,
                userProvidedSchema);
      }
      return arrowInputPartition;
    }
    selectedFields =
        schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.of());
    filter = getCombinedFilter();
    readSessionResponse = readSessionCreator.create(tableId, selectedFields, filter);
    readSession = readSessionResponse.getReadSession();
    streamList = readSession.getStreamsList();
    InputPartition[] bigQueryInputPartitions = new InputPartition[streamList.size()];
    for (int i = 0; i < streamList.size(); i++) {
      bigQueryInputPartitions[i] =
          new BigQueryInputPartition(
              bigQueryReadClientFactory,
              streamList.get(i).getName(),
              readSessionCreatorConfig.toReadRowsHelperOptions(),
              createConverter(selectedFields, readSessionResponse, userProvidedSchema));
    }
    return bigQueryInputPartitions;
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
        "No known converter for " + readSessionCreatorConfig.getReadDataFormat());
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new BigQueryPartitionReaderFactory();
  }

  public InputPartition[] createEmptyProjectionPartitions() {
    Optional<String> filter = getCombinedFilter();
    long rowCount = bigQueryClient.calculateTableSize(tableId, filter);
    logger.info("Used optimized BQ count(*) path. Count: " + rowCount);
    int partitionsCount = readSessionCreatorConfig.getDefaultParallelism();
    int partitionSize = (int) (rowCount / partitionsCount);
    InputPartition[] partitions =
        IntStream.range(0, partitionsCount)
            .mapToObj(ignored -> new BigQueryEmptyProjectInputPartition(partitionSize))
            .toArray(BigQueryEmptyProjectInputPartition[]::new);
    int firstPartitionSize = partitionSize + (int) (rowCount % partitionsCount);
    partitions[0] = new BigQueryEmptyProjectInputPartition(firstPartitionSize);
    return partitions;
  }

  private Optional<String> getCombinedFilter() {
    return emptyIfNeeded(
        SparkFilterUtils.getCompiledFilter(
            readSessionCreatorConfig.getPushAllFilters(),
            readSessionCreatorConfig.getReadDataFormat(),
            globalFilter,
            pushedFilters));
  }

  Optional<String> emptyIfNeeded(String value) {
    return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
  }

  @Override
  public StructType readSchema() {
    return schema.orElse(
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table)));
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public Statistics estimateStatistics() {
    return table.getDefinition().getType() == TableDefinition.Type.TABLE
        ? new StandardTableStatistics(table.getDefinition())
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
