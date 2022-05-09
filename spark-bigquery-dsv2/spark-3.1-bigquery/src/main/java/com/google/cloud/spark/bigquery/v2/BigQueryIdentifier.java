package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import org.apache.spark.sql.connector.catalog.Identifier;

public class BigQueryIdentifier implements Identifier {

  private final TableId tableId;

  public BigQueryIdentifier(TableId tableId) {
    this.tableId = tableId;
  }

  @Override
  public String[] namespace() {
    return new String[0];
  }

  @Override
  public String name() {
    return BigQueryUtil.friendlyTableName(tableId);
  }

  @Override
  public String toString() {
    return "BigQueryIdentifier{" + "tableId=" + BigQueryUtil.friendlyTableName(tableId) + '}';
  }
}
