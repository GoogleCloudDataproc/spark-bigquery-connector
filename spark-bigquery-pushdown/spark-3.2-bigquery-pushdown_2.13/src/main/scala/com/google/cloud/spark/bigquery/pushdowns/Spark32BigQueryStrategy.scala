package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.{SparkBigQueryUtil, SupportsQueryPushdown}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

class Spark32BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2ScanRelation is the relation that is used in the Spark 3.2 DatasourceV2 connector
    plan match {
      case scanRelation@DataSourceV2ScanRelation(_, _, _) =>
        scanRelation.scan match {
          case scan: SupportsQueryPushdown =>
            Some(SourceQuery(expressionConverter = expressionConverter,
              expressionFactory = expressionFactory,
              bigQueryRDDFactory = scan.getBigQueryRDDFactory,
              tableName = SparkBigQueryUtil.getTableNameFromOptions(scanRelation.relation.options.asInstanceOf[java.util.Map[String, String]]),
              outputAttributes = scanRelation.output,
              alias = alias.next,
              pushdownFilters = if (scan.getPushdownFilters.isPresent) Some(scan.getPushdownFilters.get) else None
            ))

          case _ => None
        }

      // We should never reach here
      case _ => None
    }
  }

  override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = {
    val queries: Seq[BigQuerySQLQuery] = children.map { child =>
      new Spark32BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory).generateQueryFromPlan(child).get
    }
    Some(UnionQuery(expressionConverter, expressionFactory, queries, alias.next))
  }
}
