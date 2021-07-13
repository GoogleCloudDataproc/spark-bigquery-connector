package com.google.cloud.spark.bigquery.acceptance;

public enum DataprocImage {
  V1_3("1.3-debian10", "2.11"),
  V1_4("1.4-debian10", "2.11"),
  V1_5("1.5-debian10", "2.12"),
  V2_0("2.0-debian10", "2.12");

  final private String imageVersion;
  final private String scalaVersion;

  DataprocImage(String imageVersion, String scalaVersion) {
    this.imageVersion = imageVersion;
    this.scalaVersion = scalaVersion;
  }

  public String getImageVersion() {
    return imageVersion;
  }

  public String getScalaVersion() {
    return scalaVersion;
  }
}
