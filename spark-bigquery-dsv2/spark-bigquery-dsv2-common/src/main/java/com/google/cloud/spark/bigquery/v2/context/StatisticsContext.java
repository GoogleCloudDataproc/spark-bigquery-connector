package com.google.cloud.spark.bigquery.v2.context;

import java.util.OptionalLong;

public interface StatisticsContext {
  OptionalLong sizeInBytes();

  OptionalLong numRows();
}
