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

import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.spark.bigquery.BigQueryRDDFactory;
import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

public class DirectBigQueryRelation extends BigQueryRelation
    implements TableScan, PrunedScan, PrunedFilteredScan {

  private final SparkBigQueryConfig options;
  private final TableInfo table;
  private final BigQueryClient bigQueryClient;
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final SQLContext sqlContext;
  private final TableDefinition defaultTableDefinition;
  private final BigQueryRDDFactory bigQueryRDDFactory;

  public static int emptyRowRDDsCreated = 0;
  private static final Logger log = LoggerFactory.getLogger(DirectBigQueryRelation.class);

  public DirectBigQueryRelation(
      SparkBigQueryConfig options,
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryReadClientFactory,
      SQLContext sqlContext) {
    super(options, table, sqlContext);
    this.options = options;
    this.table = table;
    this.bigQueryClient = bigQueryClient;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.sqlContext = sqlContext;
    this.defaultTableDefinition = table.getDefinition();
    this.bigQueryRDDFactory =
        new BigQueryRDDFactory(bigQueryClient, bigQueryReadClientFactory, options, sqlContext);
  }

  @Override
  public boolean needConversion() {
    return false;
  }

  @Override
  public long sizeInBytes() {
    return bigQueryRDDFactory.getNumBytes(defaultTableDefinition);
  }

  @Override
  public RDD<Row> buildScan() {
    return buildScan(schema().fieldNames());
  }

  @Override
  public RDD<Row> buildScan(String[] requiredColumns) {
    return buildScan(requiredColumns, new Filter[0]);
  }

  @Override
  public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
    log.info(
        "|Querying table {}, parameters sent from Spark:"
            + "|requiredColumns=[{}],"
            + "|filters=[{}]",
        getTableName(),
        String.join(",", requiredColumns),
        Arrays.stream(filters).map(f -> f.toString()).collect(Collectors.joining(",")));

    String filter = getCompiledFilter(filters);
    ReadSessionCreator readSessionCreator =
        new ReadSessionCreator(
            options.toReadSessionCreatorConfig(), bigQueryClient, bigQueryReadClientFactory);
    if (options.isOptimizedEmptyProjection() && requiredColumns.length == 0) {
      TableInfo actualTable =
          readSessionCreator.getActualTable(
              table, ImmutableList.copyOf(requiredColumns), BigQueryUtil.emptyIfNeeded(filter));
      return (RDD<Row>)
          generateEmptyRowRDD(
              actualTable, readSessionCreator.isInputTableAView(table) ? "" : filter);
    } else if (requiredColumns.length == 0) {
      log.debug("Not using optimized empty projection");
    }

    return (RDD<Row>)
        bigQueryRDDFactory.createRddFromTable(
            getTableId(), readSessionCreator, requiredColumns, filter);
  }

  @Override
  public Filter[] unhandledFilters(Filter[] filters) {
    // If a manual filter has been specified tell Spark they are all unhandled
    if (options.getFilter().isPresent()) {
      return filters;
    }
    log.debug(
        "unhandledFilters: {}",
        Arrays.stream(filters).map(f -> f.toString()).collect(Collectors.joining(" ")));
    return Iterables.toArray(
        SparkFilterUtils.unhandledFilters(
            options.getPushAllFilters(),
            options.getReadDataFormat(),
            ImmutableList.copyOf(filters)),
        Filter.class);
  }

  // VisibleForTesting
  String getCompiledFilter(Filter[] filters) {
    if (options.isCombinePushedDownFilters()) {
      // new behaviour, fixing
      // https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/74
      return SparkFilterUtils.getCompiledFilter(
          options.getPushAllFilters(), options.getReadDataFormat(), options.getFilter(), filters);
    } else {
      // old behaviour, kept for backward compatibility
      // If a manual filter has been specified do not push down anything.
      return options
          .getFilter()
          .orElse(
              SparkFilterUtils.compileFilters(
                  SparkFilterUtils.handledFilters(
                      options.getPushAllFilters(),
                      options.getReadDataFormat(),
                      ImmutableList.copyOf(filters))));
    }
  }

  private RDD<?> generateEmptyRowRDD(TableInfo tableInfo, String filter) {
    emptyRowRDDsCreated += 1;
    long numberOfRows;
    if (filter.length() == 0) {
      numberOfRows = tableInfo.getNumRows().longValue();
    } else {
      // run a query
      String table = toSqlTableReference(tableInfo.getTableId());
      String sql = "SELECT COUNT(*) from " + "`" + table + "` WHERE" + filter;
      TableResult result = bigQueryClient.query(sql);
      numberOfRows = result.iterateAll().iterator().next().get(0).getLongValue();
    }

    Function1<Object, InternalRow> objectToInternalRowConverter =
        new ObjectToInternalRowConverter();

    log.info("Used optimized BQ count(*) path. Count: {}", numberOfRows);
    return sqlContext
        .sparkContext()
        .range(0, numberOfRows, 1, sqlContext.sparkContext().defaultParallelism())
        .map(
            objectToInternalRowConverter, scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));
  }

  private static class ObjectToInternalRowConverter extends AbstractFunction1<Object, InternalRow>
      implements Serializable {

    @Override
    public InternalRow apply(Object v1) {
      return InternalRow.empty();
    }
  }

  static String toSqlTableReference(TableId tableId) {
    return tableId.getProject() + '.' + tableId.getDataset() + '.' + tableId.getTable();
  }
}
