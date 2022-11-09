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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either exss or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.direct;

import static com.google.common.truth.Truth.assertThat;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Test;

public class Scala213BigQueryRDDTest {

  @Test
  public void testCreateScala213BigQueryRDD() throws Exception {
    SparkSession sparkSession =
        SparkSession.builder().master("local").appName(getClass().getName()).getOrCreate();

    BigQueryRDDFactory factory =
        new BigQueryRDDFactory(
            null /* bigQueryClient */,
            null /* bigQueryReadClientFactory */,
            null /* bigQueryTracerFactory */,
            null /* options */,
            sparkSession.sqlContext());
    RDD<InternalRow> result =
        factory.createRDD(
            sparkSession.sqlContext(),
            null /* Partition[] */,
            null /* ReadSession */,
            null /* Schema */,
            null /* columnsInOrder */,
            null /* options */,
            null /* bigQueryReadClientFactory */,
            null /* bigQueryTracerFactory */);

    assertThat(result).isInstanceOf(Scala213BigQueryRDD.class);
  }
}
