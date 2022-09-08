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
