package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.*;

import java.util.Optional;

public class BigQueryClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        // BigQuery related
        binder.bind(BigQueryStorageClientFactory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static UserAgentHeaderProvider createUserAgentHeaderProvider(UserAgentProvider versionProvider)
    {
        return new UserAgentHeaderProvider(versionProvider.getUserAgent());
    }

    // Note that at this point the config has been validated, which means that option 2 or option 3 will always be valid
    static String calculateBillingProjectId(Optional<String> configParentProjectId, Optional<Credentials> credentials)
    {
        // 1. Get from configuration
        if (configParentProjectId.isPresent()) {
            return configParentProjectId.get();
        }
        // 2. Get from the provided credentials, but only ServiceAccountCredentials contains the project id.
        // All other credentials types (User, AppEngine, GCE, CloudShell, etc.) take it from the environment
        if (credentials.isPresent() && credentials.get() instanceof ServiceAccountCredentials) {
            return ((ServiceAccountCredentials) credentials.get()).getProjectId();
        }
        // 3. No configuration was provided, so get the default from the environment
        return BigQueryOptions.getDefaultProjectId();
    }

    @Provides
    @Singleton
    public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config)
    {
        return new BigQueryCredentialsSupplier(config.getAccessToken(), config.getCredentialsKey(), config.getCredentialsFile());
    }

    @Provides
    @Singleton
    public BigQueryClient provideBigQueryClient(BigQueryConfig config, UserAgentHeaderProvider userAgentHeaderProvider, BigQueryCredentialsSupplier bigQueryCredentialsSupplier)
    {
        String billingProjectId = calculateBillingProjectId(config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder()
                .setHeaderProvider(userAgentHeaderProvider)
                .setProjectId(billingProjectId);
        // set credentials of provided
        bigQueryCredentialsSupplier.getCredentials().ifPresent(options::setCredentials);
        return new BigQueryClient(
                options.build().getService(),
                config.getMaterializationProject(),
                config.getMaterializationDataset());
    }

}
