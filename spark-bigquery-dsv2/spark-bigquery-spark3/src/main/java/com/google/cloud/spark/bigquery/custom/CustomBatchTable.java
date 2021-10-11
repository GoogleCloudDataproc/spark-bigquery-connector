package com.google.cloud.spark.bigquery.custom;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

public class CustomBatchTable implements Table, SupportsRead {
    private  Set<TableCapability> capabilities;
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new CustomScanBuilder();
    }

    @Override
    public String name() {
        return this.getClass().getName();
    }

    @Override
    public StructType schema() {
        StructField[] structFields = new StructField[]{
                new StructField("value", DataTypes.StringType, true, Metadata.empty())};
        return new StructType(structFields);
    }

    @Override
    public Set<TableCapability> capabilities() {
        if(capabilities == null)
        {
            capabilities =new HashSet<TableCapability>();
            capabilities.add(TableCapability.BATCH_READ);
        }
        return capabilities;
    }
}
