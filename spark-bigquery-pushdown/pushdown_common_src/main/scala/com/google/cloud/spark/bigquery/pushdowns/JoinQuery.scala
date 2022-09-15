package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.JoinQuery.getConjunctionStatement
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftOuter, RightOuter}

/** The query for join operations.
 *
 * @constructor
 * @param left       The left query subtree.
 * @param right      The right query subtree.
 * @param conditions The join conditions.
 * @param joinType   The join type.
 * @param alias      Query alias.
 */
case class JoinQuery(
                      expressionConverter: SparkExpressionConverter,
                      expressionFactory: SparkExpressionFactory,
                      left: BigQuerySQLQuery,
                      right: BigQuerySQLQuery,
                      conditions: Option[Expression],
                      joinType: JoinType,
                      alias: String)
  extends BigQuerySQLQuery(expressionConverter, expressionFactory, alias, Seq(left, right), Some(
    left.outputWithQualifier ++ right.outputWithQualifier), outputAttributes = None, conjunctionStatement = ConstantString(getConjunctionStatement(joinType, left, right)).toStatement) {

  override val suffixStatement: BigQuerySQLStatement =
    conditions
      .map(ConstantString("ON") + expressionToStatement(_))
      .getOrElse(EmptyBigQuerySQLStatement())

  override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

object JoinQuery {
  def getConjunctionStatement(joinType: JoinType, left: BigQuerySQLQuery, right: BigQuerySQLQuery): String = {
    joinType match {
      case Inner =>
        // keep the nullability for both projections
        "INNER JOIN"
      case LeftOuter =>
        // Update the column's nullability of right table as true
        right.outputWithQualifier =
          right.nullableOutputWithQualifier
        "LEFT OUTER JOIN"
      case RightOuter =>
        // Update the column's nullability of left table as true
        left.outputWithQualifier =
          left.nullableOutputWithQualifier
        "RIGHT OUTER JOIN"
      case FullOuter =>
        // Update the column's nullability of both tables as true
        left.outputWithQualifier =
          left.nullableOutputWithQualifier
        right.outputWithQualifier =
          right.nullableOutputWithQualifier
        "FULL OUTER JOIN"
      case Cross =>
        "CROSS JOIN"
      case _ => throw new MatchError
    }
  }
}
