package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.blockStatement
import org.apache.spark.sql.catalyst.expressions.Expression

case class LeftSemiJoinQuery(
   expressionConverter: SparkExpressionConverter,
   expressionFactory: SparkExpressionFactory,
   left: BigQuerySQLQuery,
   right: BigQuerySQLQuery,
   conditions: Option[Expression],
   isAntiJoin: Boolean = false,
   alias: Iterator[String])
  extends BigQuerySQLQuery(expressionConverter, expressionFactory, alias.next, Seq(left), Some(left.outputWithQualifier), outputAttributes = None) {

  override val suffixStatement: BigQuerySQLStatement =
    ConstantString("WHERE") + (if (isAntiJoin) " NOT " else " ") + "EXISTS" + blockStatement(
      FilterQuery(
        expressionConverter,
        expressionFactory,
        conditions = if (conditions.isEmpty) Seq.empty else Seq(conditions.get),
        child = right,
        alias = alias.next,
        fields = Some(
          left.outputWithQualifier ++ right.outputWithQualifier
        )
      ).getStatement()
    )

  override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}
