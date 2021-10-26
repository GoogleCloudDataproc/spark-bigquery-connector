package com.google.cloud.spark.bigquery.common;

import java.io.Serializable;

public class GenericBQDataSourceReaderHelper implements Serializable {

  public boolean isBatchReadEnable() {
    return true;
  }
}
