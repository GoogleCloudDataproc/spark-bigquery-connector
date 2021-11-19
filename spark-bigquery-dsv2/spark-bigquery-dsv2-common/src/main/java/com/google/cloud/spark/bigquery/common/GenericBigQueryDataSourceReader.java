package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class GenericBigQueryDataSourceReader implements Serializable {

  private static final Logger logger =
      LoggerFactory.getLogger(GenericBigQueryDataSourceReader.class);
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
  private ReadSession readSession;
  private ImmutableList<String> selectedFields;
  private ImmutableList<String> selectedBatchFields;
  private Optional<String> filter;
  private ReadSessionResponse readSessionResponse;
  private GenericBigQuerySparkFilterHelper sparkFilterHelper;

  public GenericBigQueryDataSourceReader(
      TableInfo table,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory bigQueryTracerFactory,
      Optional<String> globalFilter,
      Optional<StructType> schema,
      String applicationId) {
    this.table = table;
    this.tableId = table.getTableId();
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
    this.selectedFields =
        this.schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.of());
    this.sparkFilterHelper = new GenericBigQuerySparkFilterHelper(table);
  }

  public void createReadSession(boolean batch) {
    this.selectedFields =
        batch
            ? this.schema
                .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
                .orElse(ImmutableList.copyOf(this.fields.keySet()))
            : this.schema
                .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
                .orElse(ImmutableList.of());
    Optional<String> filter = getCombinedFilter();
    this.readSessionResponse =
        this.readSessionCreator.create(this.tableId, this.selectedFields, filter);
    this.readSession = readSessionResponse.getReadSession();
  }

  public void emptySchemaForPartition() {
    this.selectedFields =
        schema
            .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
            .orElse(ImmutableList.copyOf(fields.keySet()));
    this.filter = getCombinedFilter();
    this.readSessionResponse = this.readSessionCreator.create(tableId, selectedFields, filter);
    this.readSession = readSessionResponse.getReadSession();
    if (this.selectedFields.isEmpty()) {
      // means select *
      Schema tableSchema =
          SchemaConverters.getSchemaWithPseudoColumns(readSessionResponse.getReadTableInfo());
      this.selectedFields =
          tableSchema.getFields().stream()
              .map(Field::getName)
              .collect(ImmutableList.toImmutableList());
    }
  }

  public Optional<StructType> getSchema() {
    return this.schema;
  }

  private void populateSchema(TableInfo table) {
    StructType convertedSchema =
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
    if (schema.isPresent()) {
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

  public BigQueryClientFactory getBigQueryReadClientFactory() {
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

  public Optional<StructType> getUserProvidedSchema() {
    return userProvidedSchema;
  }

  public ReadSession getReadSession() {
    return readSession;
  }

  public ImmutableList<String> getSelectedFields() {
    return selectedFields;
  }

  public ReadSessionResponse getReadSessionResponse() {
    return readSessionResponse;
  }

  public Map<String, StructField> getFields() {
    return this.fields;
  }

  public Optional<String> getFilter() {
    return filter;
  }

  public StructType readSchema() {
    return schema.orElse(
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(this.getTable())));
  }

  public ReadRowsResponseToInternalRowIteratorConverter createConverter() {
    logger.info(
        "Created read session for {}: {} for application id: {}",
        this.tableId.toString(),
        readSession.getName(),
        this.applicationId);
    DataFormat format = this.readSessionCreatorConfig.getReadDataFormat();
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
        "No known converter for " + this.readSessionCreatorConfig.getReadDataFormat());
  }

  private Optional<String> getCombinedFilter() {
    return emptyIfNeeded(
        SparkFilterUtils.getCompiledFilter(
            this.readSessionCreatorConfig.getPushAllFilters(),
            this.readSessionCreatorConfig.getReadDataFormat(),
            this.getGlobalFilter(),
            pushedFilters));
  }

  public boolean isEmptySchema() {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  public boolean enableBatchRead() {
    return this.readSessionCreatorConfig.getReadDataFormat() == DataFormat.ARROW
        && !isEmptySchema();
  }

  Optional<String> emptyIfNeeded(String value) {
    return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
  }

  public void pruneColumns(StructType requiredSchema) {
    this.schema = Optional.ofNullable(requiredSchema);
  }

  public Filter[] pushFilters(Filter[] filters) {
    return this.pushFilters(filters, this.readSessionCreatorConfig, this.fields);
  }

  public Filter[] pushFilters(
      Filter[] filters,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Map<String, StructField> fields) {
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
    this.pushedFilters = handledFilters.stream().toArray(Filter[]::new);
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  public Filter[] getPushedFilters() {
    return this.pushedFilters;
  }
}
