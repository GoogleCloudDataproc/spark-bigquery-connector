package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BigQueryWriteSupportDataSourceV2 implements DataSourceV2, WriteSupport {

    final Logger logger = LoggerFactory.getLogger(BigQueryWriteSupportDataSourceV2.class);

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        logger.trace("createWriter({}, {}, {}, {})", writeUUID, schema, mode, options);

        // table name and dataset-name in BigQuery
        assert options.get("table").isPresent() && options.get("database").isPresent() /*FIXME: is this the dataset name?*/;

        String tableName = options.get("table").get();
        String datasetName = options.get("database").get();
        Optional<DataSourceWriter> writer = Optional.of(new BigQueryDataSourceWriter(mode, schema, writeUUID, tableName,
                datasetName));
        logger.info("writer {} created successfully.", writeUUID);

        return writer;
    }
}
