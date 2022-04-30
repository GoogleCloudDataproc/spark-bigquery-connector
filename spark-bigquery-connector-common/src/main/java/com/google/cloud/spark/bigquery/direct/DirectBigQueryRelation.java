package com.google.cloud.spark.bigquery.direct;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.TableScan;
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

  /**
   * Default parallelism to 1 reader per 400MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending on
   * a number of factors.
   */
  private static long DEFAULT_BYTES_PER_PARTITION = 400L * 1000 * 1000;

  static int emptyRowRDDsCreated = 0;

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
  }

  @Override
  public boolean needConversion() {
    return false;
  }

  @Override
  public long sizeInBytes() {
    return getNumBytes(defaultTableDefinition);
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
    // TODO: ADD LOG STATEMENT

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
    }

    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(
            getTableId(),
            ImmutableList.copyOf(requiredColumns),
            BigQueryUtil.emptyIfNeeded(filter));
    ReadSession readSession = readSessionResponse.getReadSession();
    TableInfo actualTable = readSessionResponse.getReadTableInfo();

    List<BigQueryPartition> partitions =
        Streams.mapWithIndex(
                readSession.getStreamsList().stream(),
                (readStream, index) ->
                    new BigQueryPartition(readStream.getName(), Math.toIntExact(index)))
            .collect(Collectors.toList());

    int maxNumPartitionsRequested = getMaxNumPartitionsRequested(actualTable.getDefinition());
    // This is spammy, but it will make it clear to users the number of partitions they got and
    // why.
    if (maxNumPartitionsRequested != partitions.size()) {
      // TODO: ADD LOG STATEMENT
    }

    Set<String> requiredColumnSet = Stream.of(requiredColumns).collect(Collectors.toSet());
    Schema prunedSchema =
        Schema.of(
            SchemaConverters.getSchemaWithPseudoColumns(actualTable).getFields().stream()
                .filter(f -> requiredColumnSet.contains(f.getName()))
                .collect(Collectors.toList()));

    return (RDD<Row>)
        BigQueryRDD.scanTable(
            sqlContext,
            partitions.toArray(new BigQueryPartition[0]),
            readSession,
            prunedSchema,
            requiredColumns,
            options,
            bigQueryReadClientFactory);
  }

  private long getNumBytes(TableDefinition tableDefinition) {
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

  private RDD<? extends Serializable> generateEmptyRowRDD(TableInfo tableInfo, String filter) {
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

    Function1<Object, InternalRow> f = new MyClass();

    // Function1<Object, InternalRow> f =
    //     new AbstractFunction1<Object, InternalRow>() {
    //       @Override
    //       public InternalRow apply(Object v1) {
    //         return InternalRow.empty();
    //       }
    //     };

    // logInfo(s"Used optimized BQ count(*) path. Count: $numberOfRows")
    return sqlContext
        .sparkContext()
        .range(0, numberOfRows, 1, sqlContext.sparkContext().defaultParallelism())
        .map(f, scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));
  }

  private static class MyClass extends AbstractFunction1<Object, InternalRow>
      implements Serializable {

    @Override
    public InternalRow apply(Object v1) {
      return InternalRow.empty();
    }
  }

  private int getMaxNumPartitionsRequested(TableDefinition tableDefinition) {
    return options
        .getMaxParallelism()
        .orElse(
            Math.max(
                Math.toIntExact(getNumBytes(tableDefinition) / DEFAULT_BYTES_PER_PARTITION), 1));
  }

  static String toSqlTableReference(TableId tableId) {
    return tableId.getProject() + '.' + tableId.getDataset() + '.' + tableId.getTable();
  }
}
