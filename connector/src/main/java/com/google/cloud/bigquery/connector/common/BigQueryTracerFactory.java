package com.google.cloud.bigquery.connector.common;

/**
 * Factory to create application level tracers for bigquery operations.
 */
public interface BigQueryTracerFactory {
    BigqueryStorageReadRowsTracer newReadRowsTracer(String streamName);
}
