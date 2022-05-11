package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.types.{LongType, StringType}
import org.mockito.Mock

object TestConstants {
   val TABLE_NAME = "test_project:test_dataset.test_table"
   val SUBQUERY_0_ALIAS = "SUBQUERY_0"
   val SUBQUERY_1_ALIAS = "SUBQUERY_1"
   val SUBQUERY_2_ALIAS = "SUBQUERY_2"

   val expressionConverter: SparkExpressionConverter = new SparkExpressionConverter {}
   val schoolIdAttributeReference: AttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
   val schoolNameAttributeReference: AttributeReference = AttributeReference.apply("SchoolName", StringType)(ExprId.apply(2))

   @Mock
   var bigQueryRDDFactory: BigQueryRDDFactory = _
}
