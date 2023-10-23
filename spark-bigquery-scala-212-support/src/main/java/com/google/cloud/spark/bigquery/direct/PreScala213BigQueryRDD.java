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

import java.util.List;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Seq;
import scala.collection.Seq$;

// This method relies on the scala.Seq alias, which is different in Scala 2.12 and 2.13. In Scala
// 2.12 scala.Seq points to scala.collection.Seq whereas in Scala 2.13 it points to
// scala.collection.immutable.Seq.
class PreScala213BigQueryRDD extends RDD<InternalRow> {

  // Added suffix so that CPD wouldn't mark as duplicate
  private final BigQueryRDDContext ctx212;

  private List<String> streamNames;

  public PreScala213BigQueryRDD(SparkContext sparkContext, BigQueryRDDContext ctx) {
    super(
        sparkContext,
        (Seq<Dependency<?>>) Seq$.MODULE$.<Dependency<?>>newBuilder().result(),
        scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));

    this.ctx212 = ctx;
  }

  @Override
  public scala.collection.Iterator<InternalRow> compute(Partition split, TaskContext context) {
    return ctx212.compute(split, context);
  }

  @Override
  public Partition[] getPartitions() {
    return ctx212.getPartitions();
  }
}
