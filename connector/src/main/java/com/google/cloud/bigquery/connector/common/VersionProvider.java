package com.google.cloud.bigquery.connector.common;

@FunctionalInterface
public interface VersionProvider {

    String getVersion();

}
