package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class BQTableProvider implements TableProvider, DataSourceRegister {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getTable(null, null, options.asCaseSensitiveMap()).schema();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new BQBatchTable();
    }

    @Override
    public String shortName() {
        return "bigquery";
    }
}
