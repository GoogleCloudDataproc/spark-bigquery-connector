package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, SUBQUERY_1_ALIAS, SUBQUERY_2_ALIAS, TABLE_NAME, bigQueryRDDFactoryMock, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EmptyRow, ExprId, FrameType, Literal, RangeFrame, Rank, RowFrame, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, UnspecifiedFrame, WindowExpression, WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.types.Metadata
import org.scalatest.funsuite.AnyFunSuite

class WindowQuerySuite extends AnyFunSuite {

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  private val windowExpression = WindowExpression.apply(
    windowFunction = Rank.apply(List.apply(Literal.apply(1))),
    windowSpec = WindowSpecDefinition.apply(
      Seq(schoolNameAttributeReference),
      Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)),
      SpecifiedWindowFrame.apply(RowFrame, UnboundedPreceding, UnboundedFollowing)))

  private val windowAlias = expressionFactory.createAlias(windowExpression, "", ExprId.apply(12L), Seq.empty[String], Some(Metadata.empty))

  private val windowQuery = WindowQuery(expressionConverter, expressionFactory, Seq(windowAlias), sourceQuery, SUBQUERY_0_ALIAS)

  test("sourceStatement") {
    assert(windowQuery.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 , ( RANK () OVER ( PARTITION BY SUBQUERY_0.SCHOOLNAME ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC ) ) AS SUBQUERY_0_COL_2 FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }


}
