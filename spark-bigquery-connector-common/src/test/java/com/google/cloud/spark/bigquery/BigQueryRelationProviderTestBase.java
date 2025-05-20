/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat; // Google Truth
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Base64;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation;
import com.google.common.base.Throwables; // For getting root cause
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert; // For fail()
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class BigQueryRelationProviderTestBase {

  private static final TableId ID = TableId.of("testproject", "test_dataset", "test_table");
  private static final String TABLE_NAME_FULL = "testproject:test_dataset.test_table";
  private static final TableInfo TABLE_INFO =
      TableInfo.of(
          ID,
          StandardTableDefinition.newBuilder()
              .setSchema(Schema.of(Field.of("foo", LegacySQLTypeName.STRING)))
              .setNumBytes(42L)
              .build());

  @Mock private SQLContext sqlCtx;
  @Mock private SparkContext sc;
  private Configuration hadoopConf;
  @Mock private SparkBigQueryConfig config;
  @Mock private BigQueryClient.ReadTableOptions readTableOptions;
  @Mock private BigQueryClient bigQueryClient;
  @Mock private BigQueryClientFactory bigQueryReadClientFactory;

  private BigQueryRelationProviderBase provider;

  @Mock private Table tableMock;

  private SparkSession sparkSession;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    hadoopConf = new Configuration(false);

    when(config.getTableId()).thenReturn(ID);
    when(config.toReadTableOptions()).thenReturn(readTableOptions);

    final Injector injector =
        Guice.createInjector(
            new Module() {
              @Override
              public void configure(Binder binder) {
                binder.bind(SparkBigQueryConfig.class).toInstance(config);
                binder.bind(BigQueryClient.class).toInstance(bigQueryClient);
                binder.bind(BigQueryClientFactory.class).toInstance(bigQueryReadClientFactory);
                binder
                    .bind(
                        com.google.cloud.bigquery.connector.common.LoggingBigQueryTracerFactory
                            .class)
                    .toInstance(
                        new com.google.cloud.bigquery.connector.common
                            .LoggingBigQueryTracerFactory());
              }
            });

    GuiceInjectorCreator injectorCreator =
        new GuiceInjectorCreator() {
          @Override
          public Injector createGuiceInjector(
              SQLContext sqlContext, Map<String, String> parameters, Optional<StructType> schema) {
            return injector;
          }
        };
    provider = createProvider(() -> injectorCreator);

    when(tableMock.getTableId()).thenReturn(TABLE_INFO.getTableId());
    when(tableMock.getDefinition()).thenReturn(TABLE_INFO.getDefinition());

    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("BigQueryRelationProviderSuiteApp")
            .getOrCreate();

    when(sqlCtx.sparkContext()).thenReturn(sc);
    when(sc.hadoopConfiguration()).thenReturn(hadoopConf);
    when(sc.version()).thenReturn("2.4.0");
    when(sqlCtx.sparkSession()).thenReturn(sparkSession);
    when(sqlCtx.getAllConfs()).thenReturn(scala.collection.immutable.Map$.MODULE$.empty());
  }

  @After
  public void tearDown() {
    validateMockitoUsage();
    if (sparkSession != null) {
      sparkSession.stop();
    }
  }

  @Test
  public void tableExists() {
    when(bigQueryClient.getReadTable(any(BigQueryClient.ReadTableOptions.class)))
        .thenReturn(tableMock);

    Map<String, String> params = new HashMap<>();
    params.put("table", TABLE_NAME_FULL);
    params.put("parentProject", ID.getProject());

    BaseRelation relation = provider.createRelation(sqlCtx, params);
    assertThat(relation).isNotNull();
    assertThat(relation).isInstanceOf(DirectBigQueryRelation.class);

    verify(bigQueryClient).getReadTable(any(BigQueryClient.ReadTableOptions.class));
  }

  @Test
  public void tableDoesNotExist() {
    when(bigQueryClient.getReadTable(any(BigQueryClient.ReadTableOptions.class))).thenReturn(null);

    Map<String, String> params = new HashMap<>();
    params.put("table", TABLE_NAME_FULL);
    params.put("parentProject", ID.getProject());

    try {
      provider.createRelation(sqlCtx, params);
      Assert.fail("Expected a RuntimeException to be thrown");
    } catch (RuntimeException e) {
      assertThat(e)
          .hasMessageThat()
          .contains("Table " + BigQueryUtil.friendlyTableName(ID) + " not found");
    }
    verify(bigQueryClient).getReadTable(any(BigQueryClient.ReadTableOptions.class));
  }

  // Example of using @Test(expected) if only the exception type is of interest
  @Test(expected = RuntimeException.class)
  public void tableDoesNotExistWithExpectedExceptionAnnotation() {
    when(bigQueryClient.getReadTable(any(BigQueryClient.ReadTableOptions.class))).thenReturn(null);
    Map<String, String> params = new HashMap<>();
    params.put("table", TABLE_NAME_FULL);
    params.put("parentProject", ID.getProject());
    provider.createRelation(sqlCtx, params);
  }

  @Test
  public void credentialsParameterIsUsedToInitializeBigQueryOptions() {
    BigQueryRelationProviderBase defaultProvider = createProvider();
    String invalidCredentials = Base64.encodeBase64String("{}".getBytes());

    Map<String, String> params = new HashMap<>();
    params.put("parentProject", ID.getProject());
    params.put("credentials", invalidCredentials);
    params.put("table", TABLE_NAME_FULL);

    try {
      defaultProvider.createRelation(sqlCtx, params);
      Assert.fail("Expected an Exception to be thrown due to invalid credentials");
    } catch (Exception e) {
      // Check the root cause or the direct message
      Throwable rootCause = Throwables.getRootCause(e);
      String rootCauseMessage = rootCause.getMessage() != null ? rootCause.getMessage() : "";
      String directMessage = e.getMessage() != null ? e.getMessage() : "";

      assertThat(
              directMessage.contains("Failed to create Credentials from key")
                  || rootCauseMessage.contains("Failed to create Credentials from key")
                  || directMessage.contains("Invalid credentials")
                  || rootCauseMessage.contains("Invalid credentials"))
          .isTrue();
    }
  }

  abstract BigQueryRelationProviderBase createProvider(
      Supplier<GuiceInjectorCreator> injectorCreator);

  abstract BigQueryRelationProviderBase createProvider();
}
