package com.google.cloud.bigquery.connector.common.integration;

import com.google.cloud.bigquery.connector.common.AccessToken;
import com.google.cloud.bigquery.connector.common.AccessTokenProvider;
import java.io.IOException;

public final class ConfiguredAccessTokenProvider implements AccessTokenProvider {
    private final String config;

    public ConfiguredAccessTokenProvider(String config) {
        this.config = config;
    }

    @Override
    public AccessToken getAccessToken() throws IOException {
        return new AccessToken(config, null);
    }
}
