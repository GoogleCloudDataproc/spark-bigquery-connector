package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteSettings;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

public class BigQueryWriteClientFactory implements Serializable {
    private final Credentials credentials;
    // using the user agent as HeaderProvider is not serializable
    private final UserAgentHeaderProvider userAgentHeaderProvider;

    @Inject
    public BigQueryWriteClientFactory(
            BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
            UserAgentHeaderProvider userAgentHeaderProvider) {
        // using Guava's optional as it is serializable
        this.credentials = bigQueryCredentialsSupplier.getCredentials();
        this.userAgentHeaderProvider = userAgentHeaderProvider;
    }

    // in order to access this method from com.google.cloud.spark.bigquery this is public. TODO: make private
    public BigQueryWriteClient createBigQueryWriteClient() {
        try {
            BigQueryWriteSettings.Builder clientSettings =
                    BigQueryWriteSettings.newBuilder()
                            .setTransportChannelProvider(
                                    BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                            .setHeaderProvider(userAgentHeaderProvider)
                                            .build())
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            return BigQueryWriteClient.create(clientSettings.build());
        } catch (IOException e) {
            throw new UncheckedIOException("Error creating BigQueryStorageClient", e);
        }
    }
/*
    public BigQueryWriteClient createBigQueryWriteClient() {
        try {
            return BigQueryWriteClient.create();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize BigQueryWriteClient.", e);
        }
    }
 */
}
