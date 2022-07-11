package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.SupportsCompleteQueryPushdown
import com.google.cloud.spark.bigquery.direct.{BigQueryRDDFactory, DirectBigQueryRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class Spark24BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {

  override def generateQueryFromPlan(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      // DataSourceV2Relation is the relation that is used in the Dsv2 connector
      case l@DataSourceV2Relation(_, _, _, _, _) =>
        // Get the reader and perform reflection to get the BigQueryRddFactory
        val reader = l.newReader().asInstanceOf[SupportsCompleteQueryPushdown]
        Some(SourceQuery(expressionConverter, expressionFactory, reader.getBigQueryRDDFactory, getTableName(l), l.output, alias.next))

      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        Some(SourceQuery(expressionConverter, expressionFactory, bqRelation.getBigQueryRDDFactory, bqRelation.getTableName, l.output, alias.next))

      case _ =>  generateNonSourceQueriesFromPlan(plan)
    }
  }

  def getTableName(relation: DataSourceV2Relation): String = {
    if (relation.tableIdent.isDefined) {
      relation.tableIdent.get.table
    } else {
      relation.options("path")
    }
  }
}
