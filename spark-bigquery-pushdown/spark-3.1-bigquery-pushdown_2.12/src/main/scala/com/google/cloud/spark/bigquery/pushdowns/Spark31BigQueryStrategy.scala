package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.{BigQueryRDDFactory, DirectBigQueryRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

class Spark31BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlan(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      // DataSourceV2ScanRelation is the relation that is used in the Dsv2 Spark 3.1 connector
      case l@DataSourceV2ScanRelation(_, _, _) =>
        val scan = l.scan
        val getBigQueryRddFactoryMethod = scan.getClass.getMethod("getBigQueryRddFactory")
        Some(SourceQuery(expressionConverter, expressionFactory, getBigQueryRddFactoryMethod.invoke(scan).asInstanceOf[BigQueryRDDFactory], l.relation.options.get("table"), l.output, alias.next))

      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        Some(SourceQuery(expressionConverter, expressionFactory, bqRelation.getBigQueryRDDFactory, bqRelation.getTableName, l.output, alias.next))

      case _ =>  generateNonSourceQueriesFromPlan(plan)
    }
  }
}
