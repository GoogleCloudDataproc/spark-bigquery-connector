package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{LongType, Metadata, StructType}
import org.mockito.Mockito.when
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class Spark24ExpressionConverterSuite extends AnyFunSuite with BeforeAndAfter {
  @Mock
  var directBigQueryRelationMock: DirectBigQueryRelation = _
  @Mock
  var sparkPlanFactoryMock: SparkPlanFactory = _

  private val expressionFactory = new Spark24ExpressionFactory
  private val expressionConverter = new Spark24ExpressionConverter(expressionFactory, sparkPlanFactoryMock)
  private val fields = List(AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1), List("SUBQUERY_2")))

  before {
    MockitoAnnotations.initMocks(this)
  }

  test("convertMiscExpressions with ScalarSubquery") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")
    val logicalRelation = LogicalRelation(directBigQueryRelationMock)
    val aggregatePlan = Aggregate(Seq(), Seq(), logicalRelation)
    val scalarSubQueryExpression = ScalarSubquery.apply(aggregatePlan)
    val bigQuerySQLStatement = expressionConverter.convertMiscellaneousExpressions(scalarSubQueryExpression, fields)
    assert(bigQuerySQLStatement.isDefined)
    assert(bigQuerySQLStatement.get.toString == "( SELECT * FROM ( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 LIMIT 1 )")
  }
}
