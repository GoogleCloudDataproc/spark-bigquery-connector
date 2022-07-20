package com.google.cloud.spark.bigquery.pushdowns
import com.google.cloud.spark.bigquery.SupportsQueryPushdown
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

class Spark31BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2ScanRelation is the relation that is used in the Dsv2 connector
    plan match {
      case scanRelation@DataSourceV2ScanRelation(_, _, _) =>
        // Get the reader and cast it to SupportsQueryPushdown to get the BigQueryRDDFactory
        val scan = scanRelation.scan.asInstanceOf[SupportsQueryPushdown]

        Some(SourceQuery(expressionConverter = expressionConverter,
          expressionFactory = expressionFactory,
          bigQueryRDDFactory = scan.getBigQueryRDDFactory,
          tableName = scanRelation.relation.options.get("path"),
          outputAttributes = scanRelation.output,
          alias = alias.next,
          pushdownFilters = scan.getPushdownFilters))

      // We should never reach here
      case _ => None
    }
  }
}
