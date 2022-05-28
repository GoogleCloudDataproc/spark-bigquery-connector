package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

class Spark31BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlan(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      // TODO: Add case statement for DataSourceV2Relation once DsV2 implementation for Spark 3.1 is complete

      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        Some(SourceQuery(expressionConverter, expressionFactory, bqRelation.getBigQueryRDDFactory, bqRelation.getTableName, l.output, alias.next))

      case _ =>  generateNonSourceQueriesFromPlan(plan)
    }
  }
}
