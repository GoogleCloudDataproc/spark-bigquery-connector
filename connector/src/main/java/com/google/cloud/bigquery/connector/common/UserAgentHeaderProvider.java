package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.rpc.HeaderProvider;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

public class UserAgentHeaderProvider implements HeaderProvider, Serializable {

    private final String userAgent;

    public UserAgentHeaderProvider(String userAgent) {
        this.userAgent = userAgent;
    }

    @Override
    public Map<String, String> getHeaders() {
        return ImmutableMap.of("user-agent", userAgent);
    }
}
