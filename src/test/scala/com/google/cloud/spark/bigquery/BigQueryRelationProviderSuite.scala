/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery

import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{Matchers, Mock, MockitoAnnotations}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

class BigQueryRelationProviderSuite
    extends FunSuite
    with BeforeAndAfter
    with MockitoSugar {

  // TODO(#23) Add test case covering 'credentials' and 'credentialsFile' options

  private val ID = TableId.of("test_project", "test_dataset", "test_table")
  private val TABLE_NAME = "test_project:test_dataset.test_table"
  private val TABLE = TableInfo.of(
    ID,
    StandardTableDefinition
      .newBuilder()
      .setSchema(Schema.of(Field.of("foo", LegacySQLTypeName.STRING)))
      .setNumBytes(42L)
      .build())

  @Mock
  private var sqlCtx: SQLContext = _
  @Mock
  private var sc: SparkContext = _
  private var conf: Configuration = _
  @Mock
  private var bigQuery: BigQuery = _
  private var provider: BigQueryRelationProvider = _

  @Mock
  private var table: Table = _

  before {
    MockitoAnnotations.initMocks(this)
    conf = new Configuration(false)
    provider =
      new BigQueryRelationProvider(() => Some(bigQuery), Some(ID.getProject))
    table = TestUtils.table(TABLE)

    when(sqlCtx.sparkContext).thenReturn(sc)
    when(sc.hadoopConfiguration).thenReturn(conf)
    when(sqlCtx.getAllConfs).thenReturn(Map.empty[String, String])
  }

  after {
    // verifyNoMoreInteractions(bigQuery)
    validateMockitoUsage()
  }

  test("table exists") {
    when(bigQuery.getTable(any(classOf[TableId]))).thenReturn(table)

    val relation = provider.createRelation(sqlCtx, Map("table" -> TABLE_NAME))
    assert(relation.isInstanceOf[DirectBigQueryRelation])

    verify(bigQuery).getTable(Matchers.eq(ID))
  }

  test("table does not exist") {
    when(bigQuery.getTable(any(classOf[TableId]))).thenReturn(null)

    assertThrows[RuntimeException] {
      provider.createRelation(sqlCtx, Map("table" -> TABLE_NAME))
    }

    verify(bigQuery).getTable(Matchers.eq(ID))
  }
}
