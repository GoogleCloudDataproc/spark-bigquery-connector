package org.apache.spark.bigquery

import java.math.BigDecimal
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[spark] class BigNumericUDT extends UserDefinedType[BigNumeric]{

  override def sqlType: DataType = StringType

  override def serialize(obj: BigNumeric): InternalRow = {
    val row = new GenericInternalRow(1)
    row.update(0, UTF8String.fromString(obj.number.toPlainString))
    row
  }

  override def deserialize(datum: Any): BigNumeric = {
    if(!datum.isInstanceOf[UTF8String]) {
      throw new IllegalArgumentException(
        "Not able to deserialize, was expecting an instance of UTF8String")
    }

    val utf8str = datum.asInstanceOf[UTF8String]
    val bigNumeric = BigNumeric(new BigDecimal(utf8str.toString))
    bigNumeric
  }

  override def userClass: Class[BigNumeric] = classOf[BigNumeric]

  override def pyUDT: String =
    "com.google.cloud.spark.bigquery.big_numeric_support.BigNumericUDT"
}

@SQLUserDefinedType(udt = classOf[BigNumericUDT])
case class BigNumeric(number: BigDecimal){
}
