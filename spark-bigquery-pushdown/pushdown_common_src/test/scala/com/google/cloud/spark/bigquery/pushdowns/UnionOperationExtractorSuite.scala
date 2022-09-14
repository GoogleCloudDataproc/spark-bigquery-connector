/*
 * Copyright 2022 Google LLC
 *
 * , Version 2.0 (the "License");
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

import org.apache.spark.sql.catalyst.plans.logical.{Range, Union}
import org.scalatest.funsuite.AnyFunSuite

class UnionOperationExtractorSuite extends AnyFunSuite{

  // Need a sequence of logicalplan's to pass. So, create the simplest possible
  private val childPlan1 = Range.apply(2L, 100L, 4L, 8)
  private val childPlan2 = Range.apply(4L, 200L, 8L, 16)

  test(testName = "Union") {
    val union = Union(Seq(childPlan1, childPlan2))
    val plan = UnionOperationExtractor.unapply(union)
    assert(plan.isDefined)
    assert(plan.get.length == 2)
    assert(plan.get.head.isInstanceOf[Range])
    assert(plan.get.equals(Seq(childPlan1, childPlan2)))
  }

}
