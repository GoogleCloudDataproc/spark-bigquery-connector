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

import com.google.cloud.spark.bigquery.pushdowns.TestConstants.schoolIdAttributeReference
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite

class CastExpressionExtractorSuite extends AnyFunSuite {

  test("unapply") {
    val castExpression = Cast(schoolIdAttributeReference, StringType)
    val returnedExpression = CastExpressionExtractor.unapply(castExpression)
    assert(returnedExpression.isDefined)
    assert(returnedExpression.get.fastEquals(castExpression))

    val aliasExpression = Alias(schoolIdAttributeReference, "school-id")()
    val returnedOption = CastExpressionExtractor.unapply(aliasExpression)
    assert(returnedOption.isEmpty)
  }
}
