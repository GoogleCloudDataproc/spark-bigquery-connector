/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.SupportsQueryPushdown
import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.{LongType, Metadata, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.mockito.Mockito.when
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.Optional

class Spark31BigQueryStrategySuite extends AnyFunSuite with BeforeAndAfter {
  @Mock
  var sparkPlanFactoryMock: SparkPlanFactory = _

  @Mock
  var bigQueryRDDFactory: BigQueryRDDFactory = _

  @Mock
  var dataSourceV2Relation: DataSourceV2Relation = _

  @Mock
  var dataSourceV2ScanRelation: DataSourceV2ScanRelation = _

  val expressionFactory: SparkExpressionFactory = new SparkExpressionFactory {
    override def createAlias(child: Expression, name: String, exprId: ExprId, qualifier: Seq[String], explicitMetadata: Option[Metadata]): Alias = {
      Alias(child, name)(exprId, qualifier, explicitMetadata)
    }
  }

  val expressionConverter: SparkExpressionConverter = new Spark31ExpressionConverter(expressionFactory, sparkPlanFactoryMock)

  val schoolIdAttributeReference: AttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))

  before {
    MockitoAnnotations.initMocks(this)
  }

  test("generateQueryFromPlanForDataSourceV2 with unsupported node") {
    assert(new Spark31BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock)
      .generateQueryFromPlanForDataSourceV2(Range.apply(2L, 100L, 4L, 8)).isEmpty)
  }

  test("generateQueryFromPlanForDataSourceV2 with unsupported query pushdown scan") {
    when(dataSourceV2ScanRelation.scan).thenReturn(new MockScanWithoutQueryPushdown)

    val bigQuerySQLQuery = new Spark31BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock)
      .generateQueryFromPlanForDataSourceV2(dataSourceV2ScanRelation)

    assert(bigQuerySQLQuery.isEmpty)
  }

  test("generateQueryFromPlanForDataSourceV2 with table option set in DataSourceV2Relation node") {
    when(dataSourceV2ScanRelation.scan).thenReturn(new MockScanWithQueryPushdown)
    when(dataSourceV2ScanRelation.output).thenReturn(Seq(schoolIdAttributeReference))
    when(dataSourceV2ScanRelation.relation).thenReturn(dataSourceV2Relation)

    val caseInsensitiveStringMap = new CaseInsensitiveStringMap(new util.HashMap[String, String]() {{put("table", "MY_BIGQUERY_PROJECT.MY_BIGQUERY_DATASET.MY_BIGQUERY_TABLE")}})

    when(dataSourceV2Relation.options).thenReturn(caseInsensitiveStringMap)

    val bigQuerySQLQuery = new Spark31BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock)
      .generateQueryFromPlanForDataSourceV2(dataSourceV2ScanRelation)

    assert(bigQuerySQLQuery.isDefined)
    assert(bigQuerySQLQuery.get.getStatement().toString == "SELECT * FROM `MY_BIGQUERY_PROJECT.MY_BIGQUERY_DATASET.MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS WHERE foo = 1 AND bar = 2")
  }

  test("generateQueryFromPlanForDataSourceV2 with path option set in DataSourceV2Relation node") {
    when(dataSourceV2ScanRelation.scan).thenReturn(new MockScanWithQueryPushdown)
    when(dataSourceV2ScanRelation.output).thenReturn(Seq(schoolIdAttributeReference))
    when(dataSourceV2ScanRelation.relation).thenReturn(dataSourceV2Relation)

    val caseInsensitiveStringMap = new CaseInsensitiveStringMap(new util.HashMap[String, String]() {{put("path", "MY_BIGQUERY_PROJECT.MY_BIGQUERY_DATASET.MY_BIGQUERY_TABLE")}})

    when(dataSourceV2Relation.options).thenReturn(caseInsensitiveStringMap)

    val bigQuerySQLQuery = new Spark31BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock)
      .generateQueryFromPlanForDataSourceV2(dataSourceV2ScanRelation)

    assert(bigQuerySQLQuery.isDefined)
    assert(bigQuerySQLQuery.get.getStatement().toString == "SELECT * FROM `MY_BIGQUERY_PROJECT.MY_BIGQUERY_DATASET.MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS WHERE foo = 1 AND bar = 2")
  }

  class MockScanWithQueryPushdown extends Scan with SupportsQueryPushdown {
    override def getBigQueryRDDFactory: BigQueryRDDFactory = bigQueryRDDFactory

    override def getPushdownFilters: Optional[String] = Optional.of("foo = 1 AND bar = 2")

    override def readSchema(): StructType = null
  }

  class MockScanWithoutQueryPushdown extends Scan {
    override def readSchema(): StructType = null
  }
}
