package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.makeStatement
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/** Query for aggregation operations.
 *
 * @constructor
 * @param projectionColumns The projection columns, containing also the aggregate expressions.
 * @param groups  The grouping columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class AggregateQuery(
   expressionConverter: SparkExpressionConverter,
   expressionFactory: SparkExpressionFactory,
   projectionColumns: Seq[NamedExpression],
   groups: Seq[Expression],
   child: BigQuerySQLQuery,
   alias: String
 ) extends BigQuerySQLQuery(
  expressionConverter,
  expressionFactory,
  alias,
  children = Seq(child),
  projections = if (projectionColumns.isEmpty) None else Some(projectionColumns)) {

  /** Builds the GROUP BY clause of the filter query.
   * Insert a limit 1 to ensure that only one row returns if there are no grouping expressions
   * */
  override val suffixStatement: BigQuerySQLStatement =
    if (groups.nonEmpty) {
      ConstantString("GROUP BY") +
        makeStatement(groups.map(expressionToStatement), ",")
    } else {
      ConstantString("LIMIT 1").toStatement
    }
}
