package com.google.cloud.bigquery.connector.common;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.*;

import java.util.Optional;

public class BigQueryWriteClientModule implements Module {
    @Provides
    @Singleton
    public static UserAgentHeaderProvider createUserAgentHeaderProvider(
            UserAgentProvider versionProvider) {
        return new UserAgentHeaderProvider(versionProvider.getUserAgent());
    }

    // Note that at this point the config has been validated, which means that option 2 or option 3
    // will always be valid
    static String calculateBillingProjectId(
            Optional<String> configParentProjectId, Credentials credentials) {
        // 1. Get from configuration
        if (configParentProjectId.isPresent()) {
            return configParentProjectId.get();
        }
        // 2. Get from the provided credentials, but only ServiceAccountCredentials contains the project
        // id.
        // All other credentials types (User, AppEngine, GCE, CloudShell, etc.) take it from the
        // environment
        if (credentials instanceof ServiceAccountCredentials) {
            return ((ServiceAccountCredentials) credentials).getProjectId();
        }
        // 3. No configuration was provided, so get the default from the environment
        return BigQueryOptions.getDefaultProjectId();
    }

    @Override
    public void configure(Binder binder) {
        // BigQuery related
        binder.bind(BigQueryWriteClientFactory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config) {
        return new BigQueryCredentialsSupplier(
                config.getAccessToken(), config.getCredentialsKey(), config.getCredentialsFile());
    }

    @Provides
    @Singleton
    public BigQueryClientForWriting provideBigQueryClient(
            BigQueryConfig config,
            UserAgentHeaderProvider userAgentHeaderProvider,
            BigQueryCredentialsSupplier bigQueryCredentialsSupplier) {
        String billingProjectId =
                calculateBillingProjectId(
                        config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
        BigQueryOptions.Builder options =
                BigQueryOptions.newBuilder()
                        .setHeaderProvider(userAgentHeaderProvider)
                        .setProjectId(billingProjectId)
                        .setCredentials(bigQueryCredentialsSupplier.getCredentials());
        return new BigQueryClientForWriting(
                options.build().getService(),
                config.getMaterializationProject(),
                config.getMaterializationDataset());
    }
}
