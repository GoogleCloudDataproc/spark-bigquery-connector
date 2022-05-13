/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.spark.bigquery.direct;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for generating BigQueryRDD. Extracted this logic out so that we can reuse it from
 * 1) Dsv1 buildScan 2) Dsv1 pushdown functionality 3) Dsv2 pushdown functionality
 */
public class BigQueryRDDFactory {
  /**
   * Default parallelism to 1 reader per 400MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending on
   * a number of factors.
   */
  private static long DEFAULT_BYTES_PER_PARTITION = 400L * 1000 * 1000;

  private static final Logger log = LoggerFactory.getLogger(BigQueryRDDFactory.class);

  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig options;
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final SQLContext sqlContext;

  public BigQueryRDDFactory(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      SparkBigQueryConfig options,
      SQLContext sqlContext) {
    this.bigQueryClient = bigQueryClient;
    this.options = options;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.sqlContext = sqlContext;
  }

  /**
   * Creates RDD from the SQL string that is passed in. This functionality is invoked from the query
   * pushdown module
   */
  public RDD<InternalRow> buildScanFromSQL(String sql) {
    log.info("Materializing the following sql query to a BigQuery table: {}", sql);

    TableInfo actualTable =
        bigQueryClient.materializeQueryToTable(
            sql, options.getMaterializationExpirationTimeInMinutes());

    TableDefinition actualTableDefinition = actualTable.getDefinition();
    List<String> requiredColumns =
        actualTableDefinition.getSchema().getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());

    log.info(
        "Querying table {}, requiredColumns=[{}]",
        actualTable.getFriendlyName(),
        String.join(",", requiredColumns));

    ReadSessionCreator readSessionCreator =
        new ReadSessionCreator(
            options.toReadSessionCreatorConfig(), bigQueryClient, bigQueryReadClientFactory);

    return (RDD<InternalRow>)
        createRddFromTable(
            actualTable.getTableId(),
            readSessionCreator,
            requiredColumns.toArray(new String[0]),
            "");
  }

  // Creates BigQueryRDD from the BigQuery table that is passed in. Note that we return RDD<?>
  // instead of BigQueryRDD or RDD<InternalRow>. This is because the casting rules in Java are a lot
  // stricter than Java due to which we cannot go from RDD<InternalRow> to RDD<Row>
  public RDD<?> createRddFromTable(
      TableId tableId,
      ReadSessionCreator readSessionCreator,
      String[] requiredColumns,
      String filter) {
    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(
            tableId, ImmutableList.copyOf(requiredColumns), BigQueryUtil.emptyIfNeeded(filter));
    ReadSession readSession = readSessionResponse.getReadSession();
    TableInfo actualTable = readSessionResponse.getReadTableInfo();

    List<BigQueryPartition> partitions =
        Streams.mapWithIndex(
                readSession.getStreamsList().stream(),
                (readStream, index) ->
                    new BigQueryPartition(readStream.getName(), Math.toIntExact(index)))
            .collect(Collectors.toList());

    log.info(
        "Created read session for table '{}': {}",
        BigQueryUtil.friendlyTableName(tableId),
        readSession.getName());

    int maxNumPartitionsRequested = getMaxNumPartitionsRequested(actualTable.getDefinition());
    // This is spammy, but it will make it clear to users the number of partitions they got and
    // why.
    if (maxNumPartitionsRequested != partitions.size()) {
      log.info(
          "Requested $maxNumPartitionsRequested max partitions, but only received {} "
              + "from the BigQuery Storage API for session {}. Notice that the "
              + "number of streams in actual may be lower than the requested number, depending on "
              + "the amount parallelism that is reasonable for the table and the maximum amount of "
              + "parallelism allowed by the system.",
          partitions.size(),
          readSession.getName());
    }

    Set<String> requiredColumnSet = Stream.of(requiredColumns).collect(Collectors.toSet());
    Schema prunedSchema =
        Schema.of(
            SchemaConverters.getSchemaWithPseudoColumns(actualTable).getFields().stream()
                .filter(f -> requiredColumnSet.contains(f.getName()))
                .collect(Collectors.toList()));

    return BigQueryRDD.scanTable(
        sqlContext,
        partitions.toArray(new BigQueryPartition[0]),
        readSession,
        prunedSchema,
        requiredColumns,
        options,
        bigQueryReadClientFactory);
  }

  public long getNumBytes(TableDefinition tableDefinition) {
    TableDefinition.Type tableType = tableDefinition.getType();
    if (TableDefinition.Type.EXTERNAL == tableType
        || (options.isViewsEnabled()
            && (TableDefinition.Type.VIEW == tableType
                || TableDefinition.Type.MATERIALIZED_VIEW == tableType))) {
      return sqlContext.sparkSession().sessionState().conf().defaultSizeInBytes();
    } else {
      StandardTableDefinition standardTableDefinition = (StandardTableDefinition) tableDefinition;
      return standardTableDefinition.getNumBytes();
    }
  }

  private int getMaxNumPartitionsRequested(TableDefinition tableDefinition) {
    return options
        .getMaxParallelism()
        .orElse(
            Math.max(
                Math.toIntExact(getNumBytes(tableDefinition) / DEFAULT_BYTES_PER_PARTITION), 1));
  }
}
