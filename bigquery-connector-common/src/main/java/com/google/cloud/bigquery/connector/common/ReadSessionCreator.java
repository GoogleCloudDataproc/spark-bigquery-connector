/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNSUPPORTED;
import static java.lang.String.format;

import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A helper class, also handles view materialization
public class ReadSessionCreator {

  public static final int DEFAULT_MAX_PARALLELISM = 20_000;
  public static final int MINIMAL_PARALLELISM = 1;
  public static final int DEFAULT_MIN_PARALLELISM_FACTOR = 3;

  private static final Logger log = LoggerFactory.getLogger(ReadSessionCreator.class);

  private final ReadSessionCreatorConfig config;
  private final BigQueryClient bigQueryClient;
  private final BigQueryClientFactory bigQueryReadClientFactory;

  public ReadSessionCreator(
      ReadSessionCreatorConfig config,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory) {
    this.config = config;
    this.bigQueryClient = bigQueryClient;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
  }

  /**
   * Creates a new ReadSession for parallel reads.
   *
   * <p>Some attributes are governed by the {@link ReadSessionCreatorConfig} that this object was
   * constructed with.
   *
   * @param table The table to create the session for.
   * @param selectedFields
   * @param filter
   * @return
   */
  public ReadSessionResponse create(
      TableId table, ImmutableList<String> selectedFields, Optional<String> filter) {
    Instant sessionPrepStartTime = Instant.now();
    TableInfo tableDetails = bigQueryClient.getTable(table);

    TableInfo actualTable = getActualTable(tableDetails, selectedFields, filter);

    BigQueryReadClient bigQueryReadClient = bigQueryReadClientFactory.getBigQueryReadClient();

    String tablePath = toTablePath(actualTable.getTableId());
    CreateReadSessionRequest request =
        config
            .getRequestEncodedBase()
            .map(
                value -> {
                  try {
                    return com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest.parseFrom(
                        java.util.Base64.getDecoder().decode(value));
                  } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw new RuntimeException("Couldn't decode:" + value, e);
                  }
                })
            .orElse(CreateReadSessionRequest.newBuilder().build());
    ReadSession.Builder requestedSession = request.getReadSession().toBuilder();
    config.getTraceId().ifPresent(traceId -> requestedSession.setTraceId(traceId));

    TableReadOptions.Builder readOptions = requestedSession.getReadOptionsBuilder();
    if (!isInputTableAView(tableDetails)) {
      filter.ifPresent(readOptions::setRowRestriction);
    }
    readOptions.addAllSelectedFields(selectedFields);
    readOptions.setArrowSerializationOptions(
        ArrowSerializationOptions.newBuilder()
            .setBufferCompression(config.getArrowCompressionCodec())
            .build());

    int preferredMinStreamCount =
        config
            .getPreferredMinParallelism()
            .orElseGet(
                () -> {
                  int defaultPreferredMinStreamCount =
                      Math.max(
                          MINIMAL_PARALLELISM,
                          DEFAULT_MIN_PARALLELISM_FACTOR * config.getDefaultParallelism());
                  log.debug(
                      "using default preferred min parallelism [{}]",
                      defaultPreferredMinStreamCount);
                  return defaultPreferredMinStreamCount;
                });

    int maxStreamCount =
        config
            .getMaxParallelism()
            .orElseGet(
                () -> {
                  int defaultMaxStreamCount =
                      Math.max(ReadSessionCreator.DEFAULT_MAX_PARALLELISM, preferredMinStreamCount);
                  log.debug("using default max parallelism [{}]", defaultMaxStreamCount);
                  return defaultMaxStreamCount;
                });
    int minStreamCount = preferredMinStreamCount;
    if (minStreamCount > maxStreamCount) {
      minStreamCount = maxStreamCount;
      log.warn(
          "preferred min parallelism is larger than the max parallelism, therefore setting it to max parallelism [{}]",
          minStreamCount);
    }
    Instant sessionPrepEndTime = Instant.now();

    ReadSession readSession =
        bigQueryReadClient.createReadSession(
            request
                .newBuilder()
                .setParent("projects/" + bigQueryClient.getProjectId())
                .setReadSession(
                    requestedSession
                        .setDataFormat(config.getReadDataFormat())
                        .setReadOptions(readOptions)
                        .setTable(tablePath)
                        .build())
                .setMaxStreamCount(maxStreamCount)
                .setPreferredMinStreamCount(minStreamCount)
                .build());

    if (readSession != null) {
      Instant sessionCreationEndTime = Instant.now();
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("readSessionName", readSession.getName());
      jsonObject.addProperty("readSessionCreationStartTime", sessionPrepStartTime.toString());
      jsonObject.addProperty("readSessionCreationEndTime", sessionCreationEndTime.toString());
      jsonObject.addProperty(
          "readSessionPrepDuration",
          Duration.between(sessionPrepStartTime, sessionPrepEndTime).toMillis());
      jsonObject.addProperty(
          "readSessionCreationDuration",
          Duration.between(sessionPrepEndTime, sessionCreationEndTime).toMillis());
      jsonObject.addProperty(
          "readSessionDuration",
          Duration.between(sessionPrepStartTime, sessionCreationEndTime).toMillis());
      log.info("Read session:{}", new Gson().toJson(jsonObject));
      if (readSession.getStreamsCount() != maxStreamCount) {
        log.info(
            "Requested {} max partitions, but only received {} "
                + "from the BigQuery Storage API for session {}. Notice that the "
                + "number of streams in actual may be lower than the requested number, depending on "
                + "the amount parallelism that is reasonable for the table and the maximum amount of "
                + "parallelism allowed by the system.",
            maxStreamCount,
            readSession.getStreamsCount(),
            readSession.getName());
      }
    }

    return new ReadSessionResponse(readSession, actualTable);
  }

  String toTablePath(TableId tableId) {
    return format(
        "projects/%s/datasets/%s/tables/%s",
        tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  public TableInfo getActualTable(
      TableInfo table, ImmutableList<String> requiredColumns, Optional<String> filter) {
    String[] filters = filter.map(Stream::of).orElseGet(Stream::empty).toArray(String[]::new);
    return getActualTable(table, requiredColumns, filters);
  }

  TableInfo getActualTable(
      TableInfo table, ImmutableList<String> requiredColumns, String[] filters) {
    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();
    if (TableDefinition.Type.TABLE == tableType || TableDefinition.Type.EXTERNAL == tableType) {
      return table;
    }
    if (isInputTableAView(table)) {
      // get it from the view
      String querySql = bigQueryClient.createSql(table.getTableId(), requiredColumns, filters);
      log.debug("querySql is %s", querySql);
      return bigQueryClient.materializeViewToTable(
          querySql, table.getTableId(), config.getMaterializationExpirationTimeInMinutes());
    } else {
      // not regular table or a view
      throw new BigQueryConnectorException(
          UNSUPPORTED,
          format(
              "Table type '%s' of table '%s.%s' is not supported",
              tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
    }
  }

  public boolean isInputTableAView(TableInfo table) {
    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();

    if (TableDefinition.Type.VIEW == tableType
        || TableDefinition.Type.MATERIALIZED_VIEW == tableType) {
      if (!config.isViewsEnabled()) {
        throw new BigQueryConnectorException(
            UNSUPPORTED,
            format(
                "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                config.getViewEnabledParamName()));
      }
      return true;
    }
    return false;
  }
}
