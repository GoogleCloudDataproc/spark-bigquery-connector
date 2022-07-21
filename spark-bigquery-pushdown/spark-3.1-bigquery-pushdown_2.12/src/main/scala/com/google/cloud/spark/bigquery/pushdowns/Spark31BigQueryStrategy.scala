package com.google.cloud.spark.bigquery.pushdowns
import com.google.cloud.bigquery.connector.common.BigQueryPushdownUnsupportedException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class Spark31BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    throw new BigQueryPushdownUnsupportedException("Query pushdown for Spark 3.1 DataSourceV2 connector has not been implemented yet")
  }

  override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = {
    val queries: Seq[BigQuerySQLQuery] = children.map { child =>
      new Spark31BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory).generateQueryFromPlan(child).get
    }
    Some(UnionQuery(expressionConverter, expressionFactory, queries, alias.next))
  }
}
