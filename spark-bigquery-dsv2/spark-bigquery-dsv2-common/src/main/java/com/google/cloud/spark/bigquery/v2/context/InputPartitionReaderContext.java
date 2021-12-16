package com.google.cloud.spark.bigquery.v2.context;

import java.io.IOException;

public interface InputPartitionReaderContext<T> {

  boolean next() throws IOException;

  T get();

  void close() throws IOException;
}
