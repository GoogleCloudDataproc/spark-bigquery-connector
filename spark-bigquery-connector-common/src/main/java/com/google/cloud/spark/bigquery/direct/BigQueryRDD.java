/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigquery.direct;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.InternalRowIterator;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.AbstractIterator;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;

// Ported this class from Scala to Java with no change in functionality
class BigQueryRDD extends RDD<InternalRow> {

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
        (Seq<Dependency<?>>) Seq$.MODULE$.<Dependency<?>>newBuilder().result(),
        scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));

    this.partitions = parts;
    this.readSession = readSession;
    this.columnsInOrder = columnsInOrder;
    this.bigQueryClientFactory = bigQueryClientFactory;
    this.options = options;
    this.bqSchema = bqSchema;
  }

  @Override
  public scala.collection.Iterator<InternalRow> compute(Partition split, TaskContext context) {
    BigQueryPartition bigQueryPartition = (BigQueryPartition) split;

    ReadRowsRequest.Builder request =
        ReadRowsRequest.newBuilder().setReadStream(bigQueryPartition.getStream());

    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(
            bigQueryClientFactory,
            request,
            options.toReadSessionCreatorConfig().toReadRowsHelperOptions());
    Iterator<ReadRowsResponse> readRowsResponseIterator = readRowsHelper.readRows();

    ReadRowsResponseToInternalRowIteratorConverter converter;
    if (options.getReadDataFormat().equals(DataFormat.AVRO)) {
      converter =
          ReadRowsResponseToInternalRowIteratorConverter.avro(
              bqSchema,
              Arrays.asList(columnsInOrder),
              readSession.getAvroSchema().getSchema(),
              options.getSchema());
    } else {
      converter =
          ReadRowsResponseToInternalRowIteratorConverter.arrow(
              Arrays.asList(columnsInOrder),
              readSession.getArrowSchema().getSerializedSchema(),
              options.getSchema());
    }

    return new InterruptibleIterator<InternalRow>(
        context,
        new ScalaIterator<>(
            new InternalRowIterator(readRowsResponseIterator, converter, readRowsHelper)));
  }

  // Scala version agnostic conversion
  static class ScalaIterator<T> extends AbstractIterator<T> {

    private Iterator<T> underlying;

    public ScalaIterator(Iterator<T> underlying) {
      this.underlying = underlying;
    }

    @Override
    public boolean hasNext() {
      return underlying.hasNext();
    }

    @Override
    public T next() {
      return underlying.next();
    }
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
