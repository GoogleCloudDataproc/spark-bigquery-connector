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
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.InternalRowIterator;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.Seq$;

// This method relies on the scala.Seq alias, which is different in Scala 2.12 and 2.13. In Scala
// 2.12 scala.Seq points to scala.collection.Seq whereas in Scala 2.13 it points to
// scala.collection.immutable.Seq.
class PreScala213BigQueryRDD extends RDD<InternalRow> {

  private final Partition[] partitions;
  private final ReadSession readSession;
  private final String[] columnsInOrder;
  private final Schema bqSchema;
  private final SparkBigQueryConfig options;
  private final BigQueryClientFactory bigQueryClientFactory;
  private final BigQueryTracerFactory bigQueryTracerFactory;

  private List<String> streamNames;

  public PreScala213BigQueryRDD(
      SparkContext sparkContext,
      Partition[] parts,
      ReadSession readSession,
      Schema bqSchema,
      String[] columnsInOrder,
      SparkBigQueryConfig options,
      BigQueryClientFactory bigQueryClientFactory,
      BigQueryTracerFactory bigQueryTracerFactory) {
    super(
        sparkContext,
        (Seq<Dependency<?>>) Seq$.MODULE$.<Dependency<?>>newBuilder().result(),
        scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));

    this.partitions = parts;
    this.readSession = readSession;
    this.columnsInOrder = columnsInOrder;
    this.bigQueryClientFactory = bigQueryClientFactory;
    this.bigQueryTracerFactory = bigQueryTracerFactory;
    this.options = options;
    this.bqSchema = bqSchema;
    this.streamNames = BigQueryUtil.getStreamNames(readSession);
  }

  @Override
  public scala.collection.Iterator<InternalRow> compute(Partition split, TaskContext context) {
    BigQueryPartition bigQueryPartition = (BigQueryPartition) split;

    BigQueryStorageReadRowsTracer tracer =
        bigQueryTracerFactory.newReadRowsTracer(Joiner.on(",").join(streamNames));

    ReadRowsRequest.Builder request =
        ReadRowsRequest.newBuilder().setReadStream(bigQueryPartition.getStream());

    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(
            bigQueryClientFactory,
            request,
            options.toReadSessionCreatorConfig().toReadRowsHelperOptions(),
            Optional.of(tracer));
    Iterator<ReadRowsResponse> readRowsResponseIterator = readRowsHelper.readRows();

    StructType schema = options.getSchema().orElse(SchemaConverters.toSpark(bqSchema));

    ReadRowsResponseToInternalRowIteratorConverter converter;
    if (options.getReadDataFormat().equals(DataFormat.AVRO)) {
      converter =
          ReadRowsResponseToInternalRowIteratorConverter.avro(
              bqSchema,
              Arrays.asList(columnsInOrder),
              readSession.getAvroSchema().getSchema(),
              Optional.of(schema),
              Optional.of(tracer));
    } else {
      converter =
          ReadRowsResponseToInternalRowIteratorConverter.arrow(
              Arrays.asList(columnsInOrder),
              readSession.getArrowSchema().getSerializedSchema(),
              Optional.of(schema),
              Optional.of(tracer));
    }

    return new InterruptibleIterator<InternalRow>(
        context,
        new ScalaIterator<InternalRow>(
            new InternalRowIterator(readRowsResponseIterator, converter, readRowsHelper, tracer)));
  }

  @Override
  public Partition[] getPartitions() {
    return partitions;
  }
}
