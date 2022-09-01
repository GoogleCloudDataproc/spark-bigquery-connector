package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.{BigQueryRDDFactory, DirectBigQueryRelation}
import com.google.cloud.spark.bigquery.pushdowns.TestConstants._
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, Cast, EqualTo, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Intersect, Join, Limit, LogicalPlan, Project, Range, ReturnAnswer, Sort}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StringType, StructType}
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
    val returnedRDDFactory = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.getRDDFactory(sourceQuery)
    assert(returnedRDDFactory.isDefined)
    assert(returnedRDDFactory.get == bigQueryRDDFactoryMock)
  }

  test("exception thrown in apply method") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(sparkPlanFactoryMock.createBigQueryPlan(any(classOf[BigQuerySQLQuery]),
      any(classOf[BigQueryRDDFactory]))).thenThrow(new RuntimeException("Unable to create spark plan"))

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)
    val returnedPlan = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.apply(logicalRelation)

    assert(returnedPlan == Nil)
  }

  test("hasUnsupportedNodes with unsupported node") {
    val unsupportedNode = Intersect(childPlan, childPlan, isAll = true)
    val returnAnswerPlan = ReturnAnswer(unsupportedNode)

    assert(new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.hasUnsupportedNodes(returnAnswerPlan))
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

    assert(!new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.hasUnsupportedNodes(returnAnswerPlan))
  }

  test("generateQueryFromPlan with filter, project, limit and sort plans") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)
    val projectPlan = Project(Seq(schoolNameAttributeReference), filterPlan)
    val sortPlan = Sort(Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)), global = true, projectPlan)
    val limitPlan = Limit(Literal(10), sortPlan)

    val returnedQueryOption = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.generateQueryFromPlan(limitPlan)
    assert(returnedQueryOption.isDefined)

    val returnedQuery = returnedQueryOption.get
    assert(returnedQuery.getStatement().toString == "SELECT * FROM " +
      "( SELECT * FROM ( SELECT ( SCHOOLNAME ) AS SUBQUERY_2_COL_0 FROM " +
      "( SELECT * FROM ( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) " +
      "AS SUBQUERY_0 WHERE ( SCHOOLID = 1234 ) ) AS SUBQUERY_1 ) " +
      "AS SUBQUERY_2 ORDER BY ( SCHOOLID ) ASC ) AS SUBQUERY_3 ORDER BY ( SCHOOLID ) ASC LIMIT 10")
  }

  // Special case for Spark 2.4 in which Spark SQL query has a limit and show() is called
  test("generateQueryFromPlan with 2 limits and Sort as outermost nodes") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)
    val projectPlan = Project(Seq(schoolNameAttributeReference), filterPlan)
    val sortPlan = Sort(Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)), global = true, projectPlan)
    val innerLimitPlan = Limit(Literal(10), sortPlan)
    val outerLimitPlan = Limit(Literal(20), innerLimitPlan)

    val returnedQueryOption = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.generateQueryFromPlan(outerLimitPlan)
    assert(returnedQueryOption.isDefined)

    val returnedQuery = returnedQueryOption.get
    assert(returnedQuery.getStatement().toString == "SELECT * FROM " +
      "( SELECT * FROM ( SELECT * FROM ( SELECT ( SCHOOLNAME ) AS SUBQUERY_2_COL_0 FROM " +
      "( SELECT * FROM ( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 " +
      "WHERE ( SCHOOLID = 1234 ) ) AS SUBQUERY_1 ) AS SUBQUERY_2 " +
      "ORDER BY ( SCHOOLID ) ASC ) AS SUBQUERY_3 ORDER BY ( SCHOOLID ) ASC LIMIT 10 ) " +
      "AS SUBQUERY_4 ORDER BY ( SCHOOLID ) ASC LIMIT 20")
  }


  test("generateQueryFromPlan with aggregate plan") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val aggregateExpression = Alias.apply(AggregateExpression.apply(Count.apply(schoolIdAttributeReference), Complete, isDistinct = false), "COUNT")()
    val aggregatePlan = Aggregate(Seq(schoolNameAttributeReference), Seq(aggregateExpression), logicalRelation)

    // Need to create a new BigQueryStrategy object so as to start from the original alias
    val returnedQueryOption = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }.generateQueryFromPlan(aggregatePlan)
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
    val bigQueryStrategy = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }
    val plan = bigQueryStrategy.cleanUpLogicalPlan(projectPlan)
    val returnedQueryOption = bigQueryStrategy.generateQueryFromPlan(plan)
    assert(returnedQueryOption.isDefined)

    val returnedQuery = returnedQueryOption.get
    assert(returnedQuery.getStatement().toString == "SELECT ( COUNT ( SCHOOLID ) ) AS SUBQUERY_1_COL_0 FROM " +
      "( SELECT * FROM `MY_BIGQUERY_TABLE` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0 GROUP BY SCHOOLNAME")
  }

  test("getTopMostProjectNodeWithAliasedCasts with Aggregate before Project") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val projectPlan = Project(Nil, logicalRelation)

    val aggregateExpression = Alias.apply(AggregateExpression.apply(Count.apply(schoolIdAttributeReference), Complete, isDistinct = false), "COUNT")()
    val aggregatePlan = Aggregate(Seq(schoolNameAttributeReference), Seq(aggregateExpression), projectPlan)

    val bigQueryStrategy = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }
    assert(bigQueryStrategy.getTopMostProjectNodeWithAliasedCasts(aggregatePlan).isEmpty)
  }

  test("getTopMostProjectNodeWithAliasedCasts without Project") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val filterPlan = Filter(EqualTo.apply(schoolIdAttributeReference, Literal(1234L)), logicalRelation)

    val aggregateExpression = Alias.apply(AggregateExpression.apply(Count.apply(schoolIdAttributeReference), Complete, isDistinct = false), "COUNT")()
    val aggregatePlan = Aggregate(Seq(schoolNameAttributeReference), Seq(aggregateExpression), filterPlan)
    val returnAnswerPlan = ReturnAnswer(aggregatePlan)

    val bigQueryStrategy = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }

    assert(bigQueryStrategy.getTopMostProjectNodeWithAliasedCasts(logicalRelation).isEmpty)
    assert(bigQueryStrategy.getTopMostProjectNodeWithAliasedCasts(returnAnswerPlan).isEmpty)

    val namedRelation = new NamedRelation {
      override def name: String = "test-named-relation"

      override def output: Seq[Attribute] = Seq.empty

      override def children: Seq[LogicalPlan] = Seq.empty

      override def productElement(n: Int): Any = 1

      override def productArity: Int = 1

      override def canEqual(that: Any): Boolean = true
    }

    assert(bigQueryStrategy.getTopMostProjectNodeWithAliasedCasts(namedRelation).isEmpty)
  }

  test("getTopMostProjectNodeWithAliasedCasts should return Project") {
    when(directBigQueryRelationMock.schema).thenReturn(StructType.apply(Seq()))
    when(directBigQueryRelationMock.getTableName).thenReturn("MY_BIGQUERY_TABLE")

    val logicalRelation = LogicalRelation(directBigQueryRelationMock)

    val aggregateExpression = Alias.apply(AggregateExpression.apply(Count.apply(schoolIdAttributeReference), Complete, isDistinct = false), "COUNT")()
    val aggregatePlan = Aggregate(Seq(schoolNameAttributeReference), Seq(aggregateExpression), logicalRelation)

    val projectList = Seq(Alias(Cast(schoolIdAttributeReference, StringType), "id")(), schoolNameAttributeReference)
    val projectPlan = Project(projectList, logicalRelation)
    val returnAnswerPlan = ReturnAnswer(projectPlan)

    val bigQueryStrategy = new BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactoryMock) {
      override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = None

      override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = None
    }

    val projectNode = bigQueryStrategy.getTopMostProjectNodeWithAliasedCasts(returnAnswerPlan)
    assert(projectNode.isDefined)
    assert(projectNode.get.fastEquals(projectPlan))
  }
}
