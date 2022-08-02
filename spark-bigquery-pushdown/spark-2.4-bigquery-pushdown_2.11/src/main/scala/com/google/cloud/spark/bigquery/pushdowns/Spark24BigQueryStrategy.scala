package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.{SparkBigQueryUtil, SupportsQueryPushdown}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.collection.JavaConverters._

class Spark24BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {

  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2Relation is the relation that is used in the Spark 2.4 DatasourceV2 connector
    plan match {
      case relation@DataSourceV2Relation(_, _, _, _, _) =>
        // Get the reader and cast it to SupportsQueryPushdown to get the BigQueryRDDFactory
        relation.newReader() match {
          case reader: SupportsQueryPushdown =>
            Some(SourceQuery(expressionConverter, expressionFactory, reader.getBigQueryRDDFactory,
              SparkBigQueryUtil.getTableNameFromOptions(relation.options.asJava),
              relation.output, alias.next))

          case _ => None
        }

      // We should never reach here
      case _ => None
    }
  }

  override def createUnionQuery(children: Seq[LogicalPlan]): Option[BigQuerySQLQuery] = {
    val queries: Seq[BigQuerySQLQuery] = children.map { child =>
      new Spark24BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory).generateQueryFromPlan(child).get
    }
    Some(UnionQuery(expressionConverter, expressionFactory, queries, alias.next))
  }
}
