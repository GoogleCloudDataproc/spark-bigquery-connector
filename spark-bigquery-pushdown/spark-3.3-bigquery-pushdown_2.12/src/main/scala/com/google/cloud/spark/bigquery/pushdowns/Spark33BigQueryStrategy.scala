/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.{SparkBigQueryUtil, SupportsQueryPushdown}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

class Spark33BigQueryStrategy(expressionConverter: SparkExpressionConverter, expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory)
  extends BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory) {
  override def generateQueryFromPlanForDataSourceV2(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    // DataSourceV2ScanRelation is the relation that is used in the Spark 3.3 DatasourceV2 connector
    plan match {
      case scanRelation@DataSourceV2ScanRelation(_, _, _, _) =>
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
      new Spark33BigQueryStrategy(expressionConverter, expressionFactory, sparkPlanFactory).generateQueryFromPlan(child).get
    }
    Some(UnionQuery(expressionConverter, expressionFactory, queries, alias.next))
  }
}
