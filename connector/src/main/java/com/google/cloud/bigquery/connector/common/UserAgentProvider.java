package com.google.cloud.bigquery.connector.common;

@FunctionalInterface
public interface UserAgentProvider {

    String getUserAgent();

}
