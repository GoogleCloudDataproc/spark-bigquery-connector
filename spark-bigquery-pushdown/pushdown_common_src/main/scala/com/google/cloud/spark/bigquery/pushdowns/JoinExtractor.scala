/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.Join

// The Spark APIs for Joins are not compatible between Spark 2.4 and 3.1.
// So, we create this extractor that only extracts those parameters that are
// used for creating the join query
object JoinExtractor {
  def unapply(node: Join): Option[(JoinType, Option[Expression])] =
    node match {
      case _: Join =>
        Some(node.joinType, node.condition)

      // We should never reach here
      case _ => None
    }
}
