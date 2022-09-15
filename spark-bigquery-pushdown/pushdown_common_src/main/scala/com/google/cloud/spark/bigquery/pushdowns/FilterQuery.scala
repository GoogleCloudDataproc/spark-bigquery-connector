package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.makeStatement
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

/** Query for filter operations.
 *
 * @constructor
 * @param conditions The filter condition.
 * @param child      The child node.
 * @param alias      Query alias.
 * @param fields Contains output from the left + right query for left semi and left anti joins
 */
case class FilterQuery(
    expressionConverter: SparkExpressionConverter,
    expressionFactory: SparkExpressionFactory,
    conditions: Seq[Expression],
    child: BigQuerySQLQuery,
    alias: String,
    fields: Option[Seq[Attribute]] = None)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias,
    children = Seq(child),
    fields = fields) {

  /** Builds the WHERE statement of the filter query */
  override val suffixStatement: BigQuerySQLStatement =
    ConstantString("WHERE") + makeStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}
