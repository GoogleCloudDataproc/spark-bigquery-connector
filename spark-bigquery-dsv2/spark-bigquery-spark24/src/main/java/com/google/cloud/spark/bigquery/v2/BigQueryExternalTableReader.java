package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.spark.bigquery.SchemaConverters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class BigQueryExternalTableReader{
    private final TableInfo table;
    private final TableId tableId;
    private final BigQueryClient bigQueryClient;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final BigQueryTracerFactory bigQueryTracerFactory;
    private final Optional<String> globalFilter;
    private final String applicationId;
    private Optional<StructType> schema;
    private Optional<StructType> userProvidedSchema;
    private Filter[] pushedFilters = new Filter[] {};
    private Map<String, StructField> fields;

    BigQueryExternalTableReader(TableInfo table,
                             BigQueryClient bigQueryClient,
                             BigQueryReadClientFactory bigQueryReadClientFactory,
                             BigQueryTracerFactory tracerFactory,
                             Optional<String> globalFilter,
                             Optional<StructType> schema,
                             String applicationId){
        this.table = table;
        this.tableId = table.getTableId();
        this.bigQueryClient = bigQueryClient;
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.bigQueryTracerFactory = tracerFactory;
        this.globalFilter = globalFilter;
        StructType convertedSchema =
                SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
        if (schema.isPresent()) {
            System.out.println("in if block "+ schema.toString());
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




    }

