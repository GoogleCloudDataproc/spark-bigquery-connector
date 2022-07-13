package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.{BigQueryRDDFactory, DirectBigQueryRelation}
import com.google.cloud.spark.bigquery.pushdowns.TestConstants._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EqualTo, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Intersect, Limit, Project, Range, ReturnAnswer, Sort}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchers.any
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class BigQueryStrategySuite extends AnyFunSuite with BeforeAndAfter {
  @Mock
  private var directBigQueryRelationMock: DirectBigQueryRelation = _

  @Mock
  var sparkPlanFactoryMock: SparkPlanFactory = _

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  // Need a childPlan to pass. So, create the simplest possible
  private val childPlan = Range.apply(2L, 100L, 4L, 8)

  before {
    MockitoAnnotations.initMocks(this)
  }

  test("getRDDFactory") {
    val returnedRDDFactory = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).getRDDFactory(sourceQuery)
    assert(returnedRDDFactory.isDefined)
    assert(returnedRDDFactory.get == bigQueryRDDFactoryMock)
  }

  test("exception thrown in apply method") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(sparkPlanFactoryMock.createSparkPlan(any(classOf[BigQuerySQLQuery]),
      any(classOf[BigQueryRDDFactory]))).thenThrow(new RuntimeException("Unable to create spark plan"))

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)
    val returnedPlan = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).apply(logicalRelation)

    assert(returnedPlan == Nil)
  }

  test("hasUnsupportedNodes with unsupported node") {
    val unsupportedNode = Intersect(childPlan, childPlan, isAll = true)
    val returnAnswerPlan = ReturnAnswer(unsupportedNode)

    assert(new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).hasUnsupportedNodes(returnAnswerPlan))
  }

  test("hasUnsupportedNodes with supported nodes") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)
    val projectPlan = Project(Seq(schoolNameAttributeReference), filterPlan)
    val sortPlan = Sort(Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)), global = true, projectPlan)
    val limitPlan = Limit(Literal(10), sortPlan)
    val returnAnswerPlan = ReturnAnswer(limitPlan)

    assert(!new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).hasUnsupportedNodes(returnAnswerPlan))
  }

  test("generateQueryFromPlan with filter, project, limit and sort plans") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)
    val projectPlan = Project(Seq(schoolNameAttributeReference), filterPlan)
    val sortPlan = Sort(Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)), global = true, projectPlan)
    val limitPlan = Limit(Literal(10), sortPlan)

    val returnedQueryOption = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).generateQueryFromPlan(limitPlan)
    assert(returnedQueryOption.isDefined)

    val returnedQuery = returnedQueryOption.get
    assert(returnedQuery.getStatement().toString == "SELECT * FROM " +
      "( SELECT * FROM ( SELECT ( SCHOOLNAME ) AS SUBQUERY_2_COL_0 FROM " +
      "( SELECT * FROM ( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) " +
      "AS SUBQUERY_0 WHERE ( SCHOOLID = 1234 ) ) AS SUBQUERY_1 ) " +
      "AS SUBQUERY_2 ORDER BY ( SCHOOLID ) ASC ) AS SUBQUERY_3 ORDER BY ( SCHOOLID ) ASC LIMIT 10")
  }

  test("generateQueryFromPlan with aggregate plan") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val aggregateExpression = Alias.apply(AggregateExpression.apply(Count.apply(schoolIdAttributeReference), Complete, isDistinct = false), "COUNT")()
    val aggregatePlan = Aggregate(Seq(schoolNameAttributeReference), Seq(aggregateExpression), logicalRelation)

    // Need to create a new BigQueryStrategy object so as to start from the original alias
    val returnedQueryOption = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).generateQueryFromPlan(aggregatePlan)
    assert(returnedQueryOption.isDefined)

    val returnedQuery = returnedQueryOption.get
    assert(returnedQuery.getStatement().toString == "SELECT ( COUNT ( SCHOOLID ) ) AS SUBQUERY_1_COL_0 FROM " +
      "( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 GROUP BY SCHOOLNAME")
  }

  test("generateQueryFromPlan with empty project plan") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val aggregateExpression = Alias.apply(AggregateExpression.apply(Count.apply(schoolIdAttributeReference), Complete, isDistinct = false), "COUNT")()
    val aggregatePlan = Aggregate(Seq(schoolNameAttributeReference), Seq(aggregateExpression), logicalRelation)

    val projectPlan = Project(Nil, aggregatePlan)

    // Need to create a new BigQueryStrategy object so as to start from the original alias
    val returnedQueryOption = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock).generateQueryFromPlan(projectPlan)
    assert(returnedQueryOption.isDefined)

    val returnedQuery = returnedQueryOption.get
    assert(returnedQuery.getStatement().toString == "SELECT ( COUNT ( SCHOOLID ) ) AS SUBQUERY_1_COL_0 FROM " +
      "( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 GROUP BY SCHOOLNAME")
  }
}
