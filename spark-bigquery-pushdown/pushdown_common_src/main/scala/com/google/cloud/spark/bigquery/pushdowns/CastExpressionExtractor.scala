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

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}

// The Spark APIs for Cast are not compatible between Spark 2.4/3.1 and Spark 3.2/3.3.
// So, we create this extractor that can be used across all Spark versions
object CastExpressionExtractor {
  def unapply(node: Expression): Option[Expression] =
    node match {
      case _: Cast =>
        Some(node)

      case _ => None
    }
}
