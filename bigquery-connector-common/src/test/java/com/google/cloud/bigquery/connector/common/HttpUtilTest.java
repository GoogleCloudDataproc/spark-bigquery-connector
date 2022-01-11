package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.HeaderProvider;
import java.util.Map;
import org.junit.Test;

public class HttpUtilTest {

  @Test
  public void testCreateHeaderProviderWithParentProject() {
    BigQueryConfig config = mock(BigQueryConfig.class);
    when(config.useParentProjectForMetadataOperations()).thenReturn(true);
    when(config.getParentProjectId()).thenReturn("User-Project");

    HeaderProvider headerProvider = HttpUtil.createHeaderProvider(config, "UA");
    Map<String, String> headers = headerProvider.getHeaders();
    assertThat(headers).hasSize(2);
    assertThat(headers.get("X-Goog-User-Project")).isEqualTo("User-Project");
    assertThat(headers.get("user-agent")).isEqualTo("UA");
  }

  @Test
  public void testCreateHeaderProviderNoParentProject() {
    BigQueryConfig config = mock(BigQueryConfig.class);
    when(config.useParentProjectForMetadataOperations()).thenReturn(false);

    HeaderProvider headerProvider = HttpUtil.createHeaderProvider(config, "UA");
    Map<String, String> headers = headerProvider.getHeaders();
    assertThat(headers).hasSize(1);
    assertThat(headers.get("user-agent")).isEqualTo("UA");
  }
}
