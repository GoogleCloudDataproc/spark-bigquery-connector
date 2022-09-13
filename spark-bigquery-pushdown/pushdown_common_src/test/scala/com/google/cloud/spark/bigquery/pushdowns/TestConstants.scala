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

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{LongType, Metadata, StringType}
import org.mockito.Mock

object TestConstants {
   val TABLE_NAME = "test_project:test_dataset.test_table"
   val SUBQUERY_0_ALIAS = "SUBQUERY_0"
   val SUBQUERY_1_ALIAS = "SUBQUERY_1"
   val SUBQUERY_2_ALIAS = "SUBQUERY_2"


   val schoolIdAttributeReference: AttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
   val schoolNameAttributeReference: AttributeReference = AttributeReference.apply("SchoolName", StringType)(ExprId.apply(2))

   val expressionFactory: SparkExpressionFactory = new SparkExpressionFactory {
      override def createAlias(child: Expression, name: String, exprId: ExprId, qualifier: Seq[String], explicitMetadata: Option[Metadata]): Alias = {
         Alias(child, name)(exprId, qualifier, explicitMetadata)
      }
   }


   val expressionConverter: SparkExpressionConverter = new SparkExpressionConverter {
      // Tests for Scalar Subquery are in Spark version specific pushdown modules
      override def convertScalarSubqueryExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
         throw new UnsupportedOperationException("Scalar Subquery is supported " +
           "only from Spark version specific implementations of SparkExpressionConverter")
      }

      override def convertCheckOverflowExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
         throw new UnsupportedOperationException("CheckOverflow expression is supported " +
           "only from Spark version specific implementations of SparkExpressionConverter")
      }

      override def convertUnaryMinusExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
         throw new UnsupportedOperationException("UnaryMinus expression is supported " +
           "only from Spark version specific implementations of SparkExpressionConverter")
      }

      override def convertCastExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
         val castExpression = expression.asInstanceOf[Cast]
         performCastExpressionConversion(castExpression.child, fields, castExpression.dataType)
      }
   }

   @Mock
   var bigQueryRDDFactoryMock: BigQueryRDDFactory = _
}
