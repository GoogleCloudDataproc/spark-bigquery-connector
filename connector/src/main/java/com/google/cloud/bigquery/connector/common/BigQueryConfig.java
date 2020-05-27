package com.google.cloud.bigquery.connector.common;

import java.util.Optional;

public interface BigQueryConfig {

    Optional<String> getCredentialsKey();

    Optional<String> getCredentialsFile();

    Optional<String> getAccessToken();

    Optional<String> getParentProjectId();

    boolean isViewsEnabled();

    Optional<String> getMaterializationProject();

    Optional<String> getMaterializationDataset();
}
