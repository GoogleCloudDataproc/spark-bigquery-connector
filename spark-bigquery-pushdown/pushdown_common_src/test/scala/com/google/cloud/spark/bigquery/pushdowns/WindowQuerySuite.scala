package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.{SUBQUERY_0_ALIAS, SUBQUERY_1_ALIAS, SUBQUERY_2_ALIAS, TABLE_NAME, bigQueryRDDFactoryMock, expressionConverter, expressionFactory, schoolIdAttributeReference, schoolNameAttributeReference}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EmptyRow, ExprId, FrameType, Literal, RangeFrame, Rank, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, UnspecifiedFrame, WindowExpression, WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.types.Metadata
import org.scalatest.funsuite.AnyFunSuite

class WindowQuerySuite extends AnyFunSuite {

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)

  test("sourceStatement with row window frame with rank func") {
    val windowExpressionWithRowWindowFrameWithRankFunc = WindowExpression.apply(
      windowFunction = Rank.apply(List.apply(Literal.apply(1))),
      windowSpec = WindowSpecDefinition.apply(
        Seq(schoolNameAttributeReference),
        Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)),
        SpecifiedWindowFrame.apply(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val windowAliasWithRowsWindowFrameWithRankFunc = expressionFactory.createAlias(windowExpressionWithRowWindowFrameWithRankFunc, "", ExprId.apply(12L), Seq.empty[String], Some(Metadata.empty))
    val windowQueryWithRowsWindowFrameWithRankFunc = WindowQuery(expressionConverter, expressionFactory, Seq(windowAliasWithRowsWindowFrameWithRankFunc), sourceQuery, Some(Seq(schoolIdAttributeReference, schoolNameAttributeReference, windowAliasWithRowsWindowFrameWithRankFunc.toAttribute)), SUBQUERY_0_ALIAS)
    assert(windowQueryWithRowsWindowFrameWithRankFunc.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 , ( RANK () OVER ( PARTITION BY SUBQUERY_0.SCHOOLNAME ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC ) ) AS SUBQUERY_0_COL_2 FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }

  test("sourceStatement with row window frame with sum func") {
    val windowExpressionWithRowWindowFrameWithSumFunc = WindowExpression.apply(
      windowFunction = AggregateExpression.apply(Sum.apply(schoolIdAttributeReference), Complete, isDistinct = false),
      windowSpec = WindowSpecDefinition.apply(
        Seq(schoolNameAttributeReference),
        Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)),
        SpecifiedWindowFrame.apply(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val windowAliasWithRowsWindowFrameWithSumFunc = expressionFactory.createAlias(windowExpressionWithRowWindowFrameWithSumFunc, "", ExprId.apply(12L), Seq.empty[String], Some(Metadata.empty))
    val windowQueryWithRowsWindowFrameWithSumFunc = WindowQuery(expressionConverter, expressionFactory, Seq(windowAliasWithRowsWindowFrameWithSumFunc), sourceQuery, Some(Seq(schoolIdAttributeReference, schoolNameAttributeReference, windowAliasWithRowsWindowFrameWithSumFunc.toAttribute)), SUBQUERY_0_ALIAS)
    assert(windowQueryWithRowsWindowFrameWithSumFunc.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 , ( SUM ( SUBQUERY_0.SCHOOLID ) OVER ( PARTITION BY SUBQUERY_0.SCHOOLNAME ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) ) AS SUBQUERY_0_COL_2 FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }

  test("sourceStatement with range window frame with rowNumber func") {
    val windowExpressionWithRangeWindowFrameWithRowNum = WindowExpression.apply(
      windowFunction = RowNumber.apply(),
      windowSpec = WindowSpecDefinition.apply(
        Seq(schoolNameAttributeReference),
        Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)),
        SpecifiedWindowFrame.apply(RangeFrame, Literal.apply(-1L), Literal.apply(2L))))
    val windowAliasWithRangeWindowFrameWithRowNum = expressionFactory.createAlias(windowExpressionWithRangeWindowFrameWithRowNum, "", ExprId.apply(12L), Seq.empty[String], Some(Metadata.empty))
    val windowQueryWithRangeWindowFrameWithRowNum = WindowQuery(expressionConverter, expressionFactory, Seq(windowAliasWithRangeWindowFrameWithRowNum), sourceQuery, Some(Seq(schoolIdAttributeReference, schoolNameAttributeReference, windowAliasWithRangeWindowFrameWithRowNum.toAttribute)), SUBQUERY_0_ALIAS)
    assert(windowQueryWithRangeWindowFrameWithRowNum.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 , ( ROW_NUMBER () OVER ( PARTITION BY SUBQUERY_0.SCHOOLNAME ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC ) ) AS SUBQUERY_0_COL_2 FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }

  test("sourceStatement with range window frame with avg func") {
    val windowExpressionWithRangeWindowFrameWithAvg = WindowExpression.apply(
        windowFunction = AggregateExpression.apply(Average.apply(schoolIdAttributeReference), Complete, isDistinct = false),
        windowSpec = WindowSpecDefinition.apply(
          Seq(schoolNameAttributeReference),
          Seq(SortOrder.apply(schoolIdAttributeReference, Ascending)),
          SpecifiedWindowFrame.apply(RangeFrame, Literal.apply(-1L), Literal.apply(2L))))
    val windowAliasWithRangeWindowFrameWithAvgFunc = expressionFactory.createAlias(windowExpressionWithRangeWindowFrameWithAvg, "", ExprId.apply(12L), Seq.empty[String], Some(Metadata.empty))
    val windowQueryWithRangeWindowFrameWithAvgFunc = WindowQuery(expressionConverter, expressionFactory, Seq(windowAliasWithRangeWindowFrameWithAvgFunc), sourceQuery, Some(Seq(schoolIdAttributeReference, schoolNameAttributeReference, windowAliasWithRangeWindowFrameWithAvgFunc.toAttribute)), SUBQUERY_0_ALIAS)
    assert(windowQueryWithRangeWindowFrameWithAvgFunc.getStatement().toString == "SELECT ( SUBQUERY_0.SCHOOLID ) AS SUBQUERY_0_COL_0 , ( SUBQUERY_0.SCHOOLNAME ) AS SUBQUERY_0_COL_1 , ( AVG ( SUBQUERY_0.SCHOOLID ) OVER ( PARTITION BY SUBQUERY_0.SCHOOLNAME ORDER BY ( SUBQUERY_0.SCHOOLID ) ASC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING ) ) AS SUBQUERY_0_COL_2 FROM ( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }
}
