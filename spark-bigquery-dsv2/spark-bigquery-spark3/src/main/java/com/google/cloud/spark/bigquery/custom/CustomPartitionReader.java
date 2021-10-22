package com.google.cloud.spark.bigquery.custom;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

public class CustomPartitionReader implements PartitionReader<InternalRow> {

  private String[] inMemoryArray = {"1", "2"};
  private Iterator<String> stringIterator = Arrays.stream(inMemoryArray).iterator();

  @Override
  public boolean next() throws IOException {
    return stringIterator.hasNext();
  }

  @Override
  public InternalRow get() {
    String value = stringIterator.next();
    Object objectData = UTF8String.fromString(value);
    InternalRow row =
        InternalRow.apply(
            JavaConverters.asScalaIteratorConverter(Arrays.asList(objectData).iterator())
                .asScala()
                .toSeq());
    return row;
  }

  @Override
  public void close() throws IOException {}
}
