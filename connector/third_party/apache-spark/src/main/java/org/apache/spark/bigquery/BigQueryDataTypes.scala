package org.apache.spark.bigquery

import org.apache.spark.sql.types.{DataType, UDTRegistration}

object BigQueryDataTypes {
  val BigNumericType: DataType = new BigNumericUDT
}

