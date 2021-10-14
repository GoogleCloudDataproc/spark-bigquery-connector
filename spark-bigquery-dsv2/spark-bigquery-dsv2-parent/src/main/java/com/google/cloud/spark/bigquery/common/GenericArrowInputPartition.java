package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Optional.fromJavaUtil;

public class GenericArrowInputPartition {
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final BigQueryTracerFactory tracerFactory;
    private final List<String> streamNames;
    private final ReadRowsHelper.Options options;
    private final ImmutableList<String> selectedFields;
    private final ByteString serializedArrowSchema;
    private final com.google.common.base.Optional<StructType> userProvidedSchema;

    public GenericArrowInputPartition(
            BigQueryReadClientFactory bigQueryReadClientFactory,
            BigQueryTracerFactory tracerFactory,
            List<String> names,
            ReadRowsHelper.Options options,
            ImmutableList<String> selectedFields,
            ReadSessionResponse readSessionResponse,
            java.util.Optional<StructType> userProvidedSchema) {
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.streamNames = names;
        this.options = options;
        this.selectedFields = selectedFields;
        this.serializedArrowSchema =
                readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
        this.tracerFactory = tracerFactory;
        this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
    }



    public BigQueryReadClientFactory getBigQueryReadClientFactory() {
        return bigQueryReadClientFactory;
    }

    public BigQueryTracerFactory getTracerFactory() {
        return tracerFactory;
    }

    public List<String> getStreamNames() {
        return streamNames;
    }

    public ReadRowsHelper.Options getOptions() {
        return options;
    }

    public ImmutableList<String> getSelectedFields() {
        return selectedFields;
    }

    public ByteString getSerializedArrowSchema() {
        return serializedArrowSchema;
    }

    public Optional<StructType> getUserProvidedSchema() {
        return userProvidedSchema;
    }

}
