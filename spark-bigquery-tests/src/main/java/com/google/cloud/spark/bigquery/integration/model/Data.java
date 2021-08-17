package com.google.cloud.spark.bigquery.integration.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class Data implements Serializable {

  private String str;
  private java.sql.Timestamp ts;

  public Data(String str, Timestamp ts) {
    this.str = str;
    this.ts = ts;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }

  public Timestamp getTs() {
    return ts;
  }

  public void setTs(Timestamp ts) {
    this.ts = ts;
  }
}

