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
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
<<<<<<< HEAD
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import java.lang.reflect.Constructor;
=======
>>>>>>> f469353 (Fix)
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
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

  private static final Logger log = LoggerFactory.getLogger(BigQueryRDDFactory.class);

  private static final String QUERY_JOB_LABEL = "query_source";
  private static final String QUERY_PUSHDOWN_JOB_LABEL_VALUE = "spark_query_pushdown";

  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig options;
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory bigQueryTracerFactory;
  private final SQLContext sqlContext;

  public BigQueryRDDFactory(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory bigQueryTracerFactory,
      SparkBigQueryConfig options,
      SQLContext sqlContext) {
    this.bigQueryClient = bigQueryClient;
    this.options = options;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.bigQueryTracerFactory = bigQueryTracerFactory;
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
            sql,
            options.getMaterializationExpirationTimeInMinutes(),
            // Label to add to Query Job indicating that it was invoked as part of query pushdown
            ImmutableMap.<String, String>builder()
                .put(QUERY_JOB_LABEL, QUERY_PUSHDOWN_JOB_LABEL_VALUE)
                .build());

    TableDefinition actualTableDefinition = actualTable.getDefinition();

    // We do require requiredColumns to be passed in for pushdown. If we don't, an
    // ArrayIndexOutOfBounds
    // exception is thrown
    List<String> requiredColumns =
        actualTableDefinition.getSchema().getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());

    log.info(
        "Querying table {}, requiredColumns=[{}]",
        actualTable.getTableId().getProject()
            + ":"
            + actualTable.getTableId().getDataset()
            + ":"
            + actualTable.getTableId().getTable(),
        requiredColumns.toString());

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

  private static <T, R> Stream<R> guavaMapWithIndex(Stream<T> stream, final com.google.common.collect.Streams.FunctionWithIndex<? super T, ? extends R> function) {
    com.google.common.base.Preconditions.checkNotNull(stream);
    com.google.common.base.Preconditions.checkNotNull(function);
    boolean isParallel = stream.isParallel();
    java.util.Spliterator<T> fromSpliterator = stream.spliterator();
    Stream var10000;
    final java.util.Iterator<T> fromIterator = java.util.Spliterators.iterator(fromSpliterator);
    var10000 = java.util.stream.StreamSupport.stream(new java.util.Spliterators.AbstractSpliterator<R>(fromSpliterator.estimateSize(), fromSpliterator.characteristics() & 80) {
      long index = 0L;

      public boolean tryAdvance(java.util.function.Consumer<? super R> action) {
        if (fromIterator.hasNext()) {
          action.accept(function.apply(fromIterator.next(), (long)(this.index++)));
          return true;
        } else {
          return false;
        }
      }
    }, isParallel);
    java.util.Objects.requireNonNull(stream);
    return (Stream)var10000.onClose(stream::close);
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
        guavaMapWithIndex(
                readSession.getStreamsList().stream(),
                (readStream, index) -> new BigQueryPartition(readStream.getName(), Math.toIntExact(index)))
            .collect(Collectors.toList());

    log.info(
        "Created read session for table '{}': {}",
        BigQueryUtil.friendlyTableName(tableId),
        readSession.getName());

    Set<String> requiredColumnSet = Stream.of(requiredColumns).collect(Collectors.toSet());
    Schema prunedSchema =
        Schema.of(
            SchemaConverters.getSchemaWithPseudoColumns(actualTable).getFields().stream()
                .filter(f -> requiredColumnSet.contains(f.getName()))
                .collect(Collectors.toList()));

    return createRDD(
        sqlContext,
        partitions.toArray(new BigQueryPartition[0]),
        readSession,
        prunedSchema,
        requiredColumns,
        options,
        bigQueryReadClientFactory,
        bigQueryTracerFactory);
  }

  // Moved from BigQueryRDD.scanTable
  @VisibleForTesting
  RDD<InternalRow> createRDD(
      SQLContext sqlContext,
      Partition[] partitions,
      ReadSession readSession,
      Schema bqSchema,
      String[] columnsInOrder,
      SparkBigQueryConfig options,
      BigQueryClientFactory bigQueryClientFactory,
      BigQueryTracerFactory bigQueryTracerFactory) {
    // Unfortunately we need to use reflection here due to a cyclic dependency issue, and the fact
    // that RDD constructor dependencies are different between Scala 2.12 and Scala 2.13. In Scala
    // 2.13 `scala.collection.Seq` is mapped to `scala.collection.immutable.Seq` and this is why we
    // need two classes each compiled against different Scala version.
    //
    // The BigQueryRDD implementations have a compilation dependency on BigQueryPartition (of this
    // project), while the BigQueryRDDFactory need to initialize them, so we have a cyclic
    // dependency. In order to solve it, we use reflection to dynamically load the appropriate
    // BigQueryRDD version.
    String bigQueryRDDClassName = "com.google.cloud.spark.bigquery.direct.Scala213BigQueryRDD";
    String scalaVersion = scala.util.Properties.versionNumberString();
    if (scalaVersion.compareTo("2.13") < 0) {
      // Run uses Scala 2.11/2.12
      bigQueryRDDClassName = "com.google.cloud.spark.bigquery.direct.PreScala213BigQueryRDD";
    }
    try {
      Class<? extends RDD<InternalRow>> clazz =
          (Class<? extends RDD<InternalRow>>) Class.forName(bigQueryRDDClassName);
      Constructor<? extends RDD<InternalRow>> constructor =
          clazz.getConstructor(
              SparkContext.class,
              Partition[].class,
              ReadSession.class,
              Schema.class,
              String[].class,
              SparkBigQueryConfig.class,
              BigQueryClientFactory.class,
              BigQueryTracerFactory.class);

      RDD<InternalRow> bigQueryRDD =
          constructor.newInstance(
              sqlContext.sparkContext(),
              partitions,
              readSession,
              bqSchema,
              columnsInOrder,
              options,
              bigQueryClientFactory,
              bigQueryTracerFactory);

      return bigQueryRDD;
    } catch (Exception e) {
      throw new BigQueryConnectorException(
          String.format(
              "Could not initialize a BigQuery RDD class of type [%s}", bigQueryRDDClassName),
          e);
    }
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
}
