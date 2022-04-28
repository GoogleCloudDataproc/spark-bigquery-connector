package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;

public class BigQueryRelation extends BaseRelation {

  private final SparkBigQueryConfig options;
  private final TableInfo table;
  private final SQLContext sqlContext;
  private final TableId tableId;
  private final String tableName;

  public BigQueryRelation(SparkBigQueryConfig options, TableInfo table, SQLContext sqlContext) {
    this.options = options;
    this.table = table;
    this.sqlContext = sqlContext;
    this.tableId = table.getTableId();
    this.tableName = BigQueryUtil.friendlyTableName(tableId);
  }

  @Override
  public SQLContext sqlContext() {
    return this.sqlContext;
  }

  @Override
  public StructType schema() {
    return options
        .getSchema()
        .orElse(SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table)));
  }

  public TableId getTableId() {
    return tableId;
  }

  public String getTableName() {
    return tableName;
  }
}
