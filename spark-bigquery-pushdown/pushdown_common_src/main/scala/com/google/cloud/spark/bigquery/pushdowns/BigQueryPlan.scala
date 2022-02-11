package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructField, StructType}

/** BigQueryPlan, with RDD defined by custom query. */
case class BigQueryPlan(output: Seq[Attribute], rdd: RDD[InternalRow])
  extends SparkPlan {

  override def children: Seq[SparkPlan] = Nil

  protected override def doExecute(): RDD[InternalRow] = {

    val schema = StructType(
      output.map(attr => StructField(attr.name, attr.dataType, attr.nullable))
    )

    rdd.mapPartitions { iter =>
      val project = UnsafeProjection.create(schema)
      iter.map(project)
    }
  }
}
