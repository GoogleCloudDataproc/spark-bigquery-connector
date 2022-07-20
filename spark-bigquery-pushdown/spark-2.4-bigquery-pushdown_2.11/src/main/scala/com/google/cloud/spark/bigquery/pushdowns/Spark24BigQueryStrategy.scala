package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.SupportsQueryPushdown
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class Spark24BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {

  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2ScanRelation is the relation that is used in the Spark 2.4 DatasourceV2 connector
    plan match {
      case relation@DataSourceV2Relation(_, _, _, _, _) =>
        // Get the reader and cast it to SupportsQueryPushdown to get the BigQueryRDDFactory
        val reader = relation.newReader().asInstanceOf[SupportsQueryPushdown]

        Some(SourceQuery(expressionConverter, expressionFactory, reader.getBigQueryRDDFactory,
          // relation.tableIdent.get.table is set when the "table" option is used, relation.options("path") is set when .load("table_name) is used
          if (relation.tableIdent.isDefined) relation.tableIdent.get.table else relation.options("path"),
          relation.output, alias.next))

      // We should never reach here
      case _ => None
    }
  }
}
