package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.cloud.spark.bigquery.common.GenericBQDataSourceReaderHelper;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDataSourceReader;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import java.util.*;

public class NewBigQueryScanBuilder extends GenericBigQueryDataSourceReader implements ScanBuilder,
        SupportsPushDownFilters,
        SupportsPushDownRequiredColumns,
        SupportsReportStatistics,Scan,Batch
{
    private static final Logger logger = LoggerFactory.getLogger(NewBigQueryScanBuilder.class);

    private Optional<StructType> schema;
    private Optional<StructType> userProvidedSchema;
    private Filter[] pushedFilters = new Filter[] {};
    private Map<String, StructField> fields;
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
    public NewBigQueryScanBuilder(
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
    public Scan build() {
        return null;
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

    @Override
    public Statistics estimateStatistics() {
         return super.getTable().getDefinition().getType() == TableDefinition.Type.TABLE
                ? new com.google.cloud.spark.bigquery.v2.StandardTableStatistics(super.getTable().getDefinition())
                : UNKNOWN_STATISTICS;
    }

    @Override
    public StructType readSchema() {
        return schema.orElse(
                SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(super.getTable())));
    }

    @Override
    public Batch toBatch() {
        return this;
    }
    private Optional<String> getCombinedFilter() {
        return emptyIfNeeded(
                SparkFilterUtils.getCompiledFilter(
                        super.getReadSessionCreatorConfig().getPushAllFilters(),
                        super.getReadSessionCreatorConfig().getReadDataFormat(),
                        super.getGlobalFilter(),
                        pushedFilters));
    }
    Optional<String> emptyIfNeeded(String value) {
        return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
    }

    @Override
    public InputPartition[] planInputPartitions() {

        ImmutableList<String> selectedFields =
                schema
                        .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
                        .orElse(ImmutableList.of());
        Optional<String> filter = getCombinedFilter();
        ReadSessionResponse readSessionResponse =
                super.getReadSessionCreator().create(super.getTableId(), selectedFields, filter);
        ReadSession readSession = readSessionResponse.getReadSession();
        GenericBQDataSourceReaderHelper readerHelper= new GenericBQDataSourceReaderHelper();
        logger.info(
                "Created read session for {}: {} for application id: {}",
                super.getTableId().toString(),
                readSession.getName(),
                super.getApplicationId());


        return readSession.getStreamsList().stream()
                .map(
                        stream ->
                                new NewBigQueryInputPartition(
                                        super.getBigQueryReadClientFactory(),
                                        stream.getName(),
                                        super.getReadSessionCreatorConfig().toReadRowsHelperOptions(),
                                        readerHelper.createConverter(selectedFields, readSessionResponse, userProvidedSchema,super.getReadSessionCreatorConfig()))).toArray(InputPartition[]::new);

        //return new InputPartition[0];
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return null;
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
}

