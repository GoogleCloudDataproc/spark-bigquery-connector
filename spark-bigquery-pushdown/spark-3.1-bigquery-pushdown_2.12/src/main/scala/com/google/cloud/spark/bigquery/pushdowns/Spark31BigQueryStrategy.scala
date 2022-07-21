package com.google.cloud.spark.bigquery.pushdowns
import com.google.cloud.spark.bigquery.SupportsQueryPushdown
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.getTableNameFromOptions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import scala.collection.JavaConverters._

class Spark31BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2ScanRelation is the relation that is used in the Spark 3.1 DatasourceV2 connector
    plan match {
      case scanRelation@DataSourceV2ScanRelation(_, _, _) =>
        // Get the scan and cast it to SupportsQueryPushdown to get the BigQueryRDDFactory
        val scan = scanRelation.scan.asInstanceOf[SupportsQueryPushdown]

        Some(SourceQuery(expressionConverter = expressionConverter,
          expressionFactory = expressionFactory,
          bigQueryRDDFactory = scan.getBigQueryRDDFactory,
          tableName = getTableNameFromOptions(scanRelation.relation.options.asScala.toMap),
          outputAttributes = scanRelation.output,
          alias = alias.next,
          pushdownFilters = if (scan.getPushdownFilters.isPresent) Some(scan.getPushdownFilters.get) else None
        ))

      // We should never reach here
      case _ => None
    }
  }

  override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = {
    val queries: Seq[BigQuerySQLQuery] = children.map { child =>
      new Spark31BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory).generateQueryFromPlan(child).get
    }
    Some(UnionQuery(expressionConverter, expressionFactory, queries, alias.next))
  }
}
