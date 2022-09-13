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
