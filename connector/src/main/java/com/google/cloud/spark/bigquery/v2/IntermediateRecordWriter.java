package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

public interface IntermediateRecordWriter {

  void write(InternalRow record) throws IOException;

  void close() throws IOException;

  String getPath();
}
