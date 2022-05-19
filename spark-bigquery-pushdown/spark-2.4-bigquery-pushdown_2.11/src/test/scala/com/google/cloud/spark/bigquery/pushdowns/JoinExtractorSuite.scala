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

import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, Range}
import org.scalatest.funsuite.AnyFunSuite

class JoinExtractorSuite extends AnyFunSuite {
  // Need a childPlan to pass. So, create the simplest possible
  private val leftChildPlan = Range.apply(2L, 100L, 4L, 8)

  // Need a childPlan to pass. So, create the simplest possible
  private val rightChildPlan = Range.apply(2L, 100L, 4L, 16)

  test("unapply") {
    val joinExpression = EqualTo.apply(Literal(leftChildPlan.start), Literal(rightChildPlan.start))
    val joinPlan = Join.apply(leftChildPlan, rightChildPlan, JoinType.apply("inner"), Option(joinExpression))
    val returnedOption = JoinExtractor.unapply(joinPlan)
    assert(returnedOption.isDefined)
    assert(returnedOption.get._1 == JoinType.apply("inner"))
    assert(returnedOption.get._2 == Option(joinExpression))
  }
}
