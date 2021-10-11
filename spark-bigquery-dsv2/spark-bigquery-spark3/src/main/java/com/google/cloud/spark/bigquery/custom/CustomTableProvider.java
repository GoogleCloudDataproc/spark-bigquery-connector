package com.google.cloud.spark.bigquery.custom;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CustomTableProvider implements TableProvider {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getTable(null, null, options.asCaseSensitiveMap()).schema();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new CustomBatchTable();
    }
}
