package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import java.util.HashSet;
import java.util.Set;

public class NewBigQueryTable implements Table, SupportsRead, SupportsWrite {
    private Set<TableCapability> capabilities;
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public StructType schema() {
        return null;
    }

    @Override
    public Set<TableCapability> capabilities() {
         if (capabilities == null) {
            capabilities = new HashSet<TableCapability>();
            capabilities.add(TableCapability.BATCH_READ);
        }
        return capabilities;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return null;
    }
}
