package com.google.cloud.spark.bigquery.pushdowns
import com.google.cloud.spark.bigquery.SupportsQueryPushdown
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class Spark31BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2ScanRelation is the relation that is used in the Spark 3.1 DatasourceV2 connector
    plan match {
      case scanRelation@DataSourceV2ScanRelation(_, _, _) =>
        // Get the scan and cast it to SupportsQueryPushdown to get the BigQueryRDDFactory
        val scan = scanRelation.scan.asInstanceOf[SupportsQueryPushdown]

        // Get the passed in options to get the table name
        val options: CaseInsensitiveStringMap = scanRelation.relation.options

        Some(SourceQuery(expressionConverter = expressionConverter,
          expressionFactory = expressionFactory,
          bigQueryRDDFactory = scan.getBigQueryRDDFactory,
          // options.get("table") when the "table" option is used, options.get("path") is set when .load("table_name) is used
          tableName = if (options.containsKey("path")) options.get("path") else options.get("table"),
          outputAttributes = scanRelation.output,
          alias = alias.next,
          pushdownFilters = scan.getPushdownFilters))

      // We should never reach here
      case _ => None
    }
  }
}
