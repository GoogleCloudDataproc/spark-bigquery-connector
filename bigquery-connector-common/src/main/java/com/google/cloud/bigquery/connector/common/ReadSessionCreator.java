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
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A helper class, also handles view materialization
public class ReadSessionCreator {
  /**
   * Default parallelism to 1 reader per 64MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending on
   * a number of factors. Does not apply to external tables.
   */
  public static final int DEFAULT_BYTES_PER_PARTITION = 256 * 1000 * 1000;

  public static final int DEFAULT_MAX_PARALLELISM = 10_000;

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

    int maxStreamCount =
        config
            .getMaxParallelism()
            .orElseGet(
                () -> {
                  int defaultParallelismForTable =
                      calculateDefaultMaxParallelismForTable(actualTable.getDefinition());
                  log.debug("using default parallelism [{}]", defaultParallelismForTable);
                  return defaultParallelismForTable;
                });

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
                .build());

    if (readSession != null && readSession.getStreamsCount() != maxStreamCount) {
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

    return new ReadSessionResponse(readSession, actualTable);
  }

  private int calculateDefaultMaxParallelismForTable(TableDefinition tableDefinition) {
    if (tableDefinition instanceof StandardTableDefinition) {
      StandardTableDefinition standardTableDefinition = (StandardTableDefinition) tableDefinition;
      return Math.max(
          (int) (standardTableDefinition.getNumBytes() / DEFAULT_BYTES_PER_PARTITION), 1);
    }
    return DEFAULT_MAX_PARALLELISM;
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
