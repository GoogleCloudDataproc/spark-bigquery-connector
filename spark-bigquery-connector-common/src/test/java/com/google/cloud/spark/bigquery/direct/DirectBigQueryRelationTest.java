/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.direct;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStub;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class DirectBigQueryRelationTest {

  private static final String PARENT_PROJECT = "parent-project";
  private static final TableId TABLE_ID = TableId.of("project", "dataset", "table");
  private static final TableInfo TABLE =
      TableInfo.newBuilder(
              TABLE_ID,
              StandardTableDefinition.newBuilder()
                  .setSchema(Schema.of(Field.of("name", StandardSQLTypeName.STRING)))
                  .setNumBytes(1L)
                  .setNumRows(1L)
                  .build())
          .build();

  private static SparkSession sparkSession;

  private SparkBigQueryConfig options;
  private BigQueryClient bigQueryClient;
  private BigQueryClientFactory bigQueryReadClientFactory;
  private UnaryCallable<CreateReadSessionRequest, ReadSession> createReadSessionCall;

  @BeforeClass
  public static void createSparkSession() {
    sparkSession =
        SparkSession.builder()
            .master("local")
            .appName(DirectBigQueryRelationTest.class.getName())
            .getOrCreate();
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    options = mock(SparkBigQueryConfig.class);
    when(options.getTableId()).thenReturn(TABLE_ID);
    when(options.isOptimizedEmptyProjection()).thenReturn(true);
    when(options.toReadSessionCreatorConfig())
        .thenReturn(
            new ReadSessionCreatorConfigBuilder().setEnableReadSessionCaching(false).build());

    bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.getTable(any())).thenReturn(TABLE);
    when(bigQueryClient.getProjectId()).thenReturn(PARENT_PROJECT);

    EnhancedBigQueryReadStub stub = mock(EnhancedBigQueryReadStub.class);
    createReadSessionCall = mock(UnaryCallable.class);
    when(stub.createReadSessionCallable()).thenReturn(createReadSessionCall);
    when(createReadSessionCall.call(any()))
        .thenReturn(
            ReadSession.newBuilder()
                .setName("session")
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
    BigQueryReadClient readClient = BigQueryReadClient.create(stub);
    FieldUtils.writeField(readClient, "settings", BigQueryReadSettings.newBuilder().build(), true);

    bigQueryReadClientFactory = mock(BigQueryClientFactory.class);
    when(bigQueryReadClientFactory.getBigQueryReadClient()).thenReturn(readClient);
  }

  /**
   * Regression test for
   * https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/1352.
   *
   * <p>An empty projection (e.g. {@code df.count()} or {@code df.isEmpty()}) takes the optimized
   * path, which runs a {@code SELECT COUNT(*)} query job. A principal lacking {@code
   * bigquery.jobs.create} gets a 403 from that job. The connector must surface an error that names
   * the missing permission and the {@code optimizedEmptyProjection} option.
   */
  @Test
  public void testEmptyProjectionWithoutJobsCreatePermissionFailsWithActionableError() {
    BigQueryException accessDenied = accessDeniedException();
    when(bigQueryClient.calculateTableSize(any(TableInfo.class), any(), anyBoolean()))
        .thenThrow(accessDenied);

    DirectBigQueryRelation relation = createRelation();
    BigQueryConnectorException thrown =
        assertThrows(
            BigQueryConnectorException.class,
            () -> relation.buildScan(new String[0], new Filter[0]));

    assertThat(thrown).hasMessageThat().contains("bigquery.jobs.create");
    assertThat(thrown).hasMessageThat().contains("project " + PARENT_PROJECT);
    assertThat(thrown).hasMessageThat().contains("optimizedEmptyProjection");
    assertThat(thrown).hasMessageThat().contains("allowStaleCountFromMetadata");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(accessDenied);
    verify(createReadSessionCall, never()).call(any());
  }

  @Test
  public void testEmptyProjectionPropagatesOtherErrorsUnchanged() {
    BigQueryException invalidQuery =
        new BigQueryException(
            400, "Invalid query", new BigQueryError("invalidQuery", "global", "Invalid query"));
    when(bigQueryClient.calculateTableSize(any(TableInfo.class), any(), anyBoolean()))
        .thenThrow(invalidQuery);

    DirectBigQueryRelation relation = createRelation();
    BigQueryException thrown =
        assertThrows(
            BigQueryException.class, () -> relation.buildScan(new String[0], new Filter[0]));

    assertThat(thrown).isSameInstanceAs(invalidQuery);
  }

  @Test
  public void testEmptyProjectionUsesOptimizedCount() {
    when(bigQueryClient.calculateTableSize(any(TableInfo.class), any(), anyBoolean()))
        .thenReturn(5L);

    RDD<Row> result = createRelation().buildScan(new String[0], new Filter[0]);

    assertThat(result.count()).isEqualTo(5L);
    verify(bigQueryClient).calculateTableSize(eq(TABLE), eq(Optional.empty()), eq(false));
    verify(createReadSessionCall, never()).call(any());
  }

  @Test
  public void testEmptyProjectionPassesAllowStaleCountFromMetadata() {
    when(options.isAllowStaleCountFromMetadata()).thenReturn(true);
    when(bigQueryClient.calculateTableSize(any(TableInfo.class), any(), anyBoolean()))
        .thenReturn(5L);

    createRelation().buildScan(new String[0], new Filter[0]);

    verify(bigQueryClient).calculateTableSize(eq(TABLE), eq(Optional.empty()), eq(true));
  }

  /**
   * With {@code optimizedEmptyProjection=false}, the documented workaround for principals without
   * {@code bigquery.jobs.create}, no query job is run and the rows are read through the Storage
   * Read API.
   */
  @Test
  public void testEmptyProjectionWithOptimizationDisabledUsesStorageReadApi() {
    when(options.isOptimizedEmptyProjection()).thenReturn(false);
    when(bigQueryClient.calculateTableSize(any(TableInfo.class), any(), anyBoolean()))
        .thenThrow(accessDeniedException());

    RDD<Row> result = createRelation().buildScan(new String[0], new Filter[0]);

    assertThat(result).isNotNull();
    verify(bigQueryClient, never()).calculateTableSize(any(TableInfo.class), any(), anyBoolean());
    ArgumentCaptor<CreateReadSessionRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateReadSessionRequest.class);
    verify(createReadSessionCall, times(1)).call(requestCaptor.capture());
    ReadSession requestedSession = requestCaptor.getValue().getReadSession();
    assertThat(requestedSession.getTable()).isEqualTo(TABLE_ID.getIAMResourceName());
    assertThat(requestedSession.getReadOptions().getSelectedFieldsList()).isEmpty();
  }

  private DirectBigQueryRelation createRelation() {
    return new DirectBigQueryRelation(
        options,
        TABLE,
        bigQueryClient,
        bigQueryReadClientFactory,
        mock(BigQueryTracerFactory.class),
        sparkSession.sqlContext());
  }

  private static BigQueryException accessDeniedException() {
    String message =
        "Access Denied: Project parent-project: User does not have bigquery.jobs.create permission"
            + " in project parent-project.";
    return new BigQueryException(
        403, message, new BigQueryError("accessDenied", "global", message));
  }
}
