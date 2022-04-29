package com.google.cloud.spark.bigquery.direct;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.InternalRowIterator;
import com.google.cloud.spark.bigquery.ReadRowsResponseToRowIteratorConverter;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.JavaConverters;

public class BigQueryRDD extends RDD<Row> {

  private final Partition[] partitions;
  private final ReadSession readSession;
  private final String[] columnsInOrder;
  private final Schema bqSchema;
  private final SparkBigQueryConfig options;
  private final BigQueryClientFactory bigQueryClientFactory;

  public BigQueryRDD(
      SparkContext sparkContext,
      Partition[] parts,
      ReadSession readSession,
      Schema bqSchema,
      String[] columnsInOrder,
      SparkBigQueryConfig options,
      BigQueryClientFactory bigQueryClientFactory) {
    super(
        sparkContext,
        JavaConverters.collectionAsScalaIterableConverter(new ArrayList<Dependency<?>>())
            .asScala()
            .toSeq(),
        scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));

    this.partitions = parts;
    this.readSession = readSession;
    this.columnsInOrder = columnsInOrder;
    this.bigQueryClientFactory = bigQueryClientFactory;
    this.options = options;
    this.bqSchema = bqSchema;
  }

  @Override
  public scala.collection.Iterator<Row> compute(Partition split, TaskContext context) {
    BigQueryPartition bqPartition = (BigQueryPartition) split;
    ReadRowsRequest.Builder request =
        ReadRowsRequest.newBuilder().setReadStream(bqPartition.getStream());
    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(
            bigQueryClientFactory,
            request,
            options.toReadSessionCreatorConfig().toReadRowsHelperOptions());
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
    ReadRowsResponseToRowIteratorConverter converter;
    if (options.getReadDataFormat().equals(DataFormat.AVRO)) {
      converter =
          ReadRowsResponseToRowIteratorConverter.avro(
              bqSchema,
              Arrays.asList(columnsInOrder),
              readSession.getAvroSchema().getSchema(),
              options.getSchema());
    } else {
      converter =
          ReadRowsResponseToRowIteratorConverter.arrow(
              Arrays.asList(columnsInOrder),
              readSession.getArrowSchema().getSerializedSchema(),
              options.getSchema());
    }

    return new InterruptibleIterator<Row>(
        context,
        JavaConverters.asScalaIteratorConverter(
                new InternalRowIterator(readRowsResponses, converter, readRowsHelper))
            .asScala());
  }

  @Override
  public Partition[] getPartitions() {
    return partitions;
  }

  public static BigQueryRDD scanTable(
      SQLContext sqlContext,
      Partition[] partitions,
      ReadSession readSession,
      Schema bqSchema,
      String[] columnsInOrder,
      SparkBigQueryConfig options,
      BigQueryClientFactory bigQueryClientFactory) {
    return new BigQueryRDD(
        sqlContext.sparkContext(),
        partitions,
        readSession,
        bqSchema,
        columnsInOrder,
        options,
        bigQueryClientFactory);
  }
}

class BigQueryPartition implements Partition {
  private final String stream;
  private final int index;

  public BigQueryPartition(String stream, int index) {
    this.stream = stream;
    this.index = index;
  }

  public String getStream() {
    return stream;
  }

  @Override
  public int index() {
    return index;
  }
}
