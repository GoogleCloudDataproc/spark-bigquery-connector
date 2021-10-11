package com.google.cloud.spark.bigquery.custom;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;

public class CustomScanBuilder implements ScanBuilder {
    @Override
    public Scan build() {
        return new CustomScanWithBatch();
    }
}
