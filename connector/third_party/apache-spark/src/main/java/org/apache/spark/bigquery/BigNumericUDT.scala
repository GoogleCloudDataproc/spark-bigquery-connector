package org.apache.spark.bigquery

import java.math.BigDecimal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[spark] class BigNumericUDT extends UserDefinedType[BigNumeric]{
  override def sqlType: DataType = StringType //DataTypes.createDecimalType

  override def serialize(obj: BigNumeric): InternalRow = {
    val row = new GenericInternalRow(1)
    row.update(0, UTF8String.fromString(obj.number.toPlainString))
    System.out.println("serialize:   " + obj.number.toPlainString)
    row
  }

  override def deserialize(datum: Any): BigNumeric = {
    val utf8str = datum.asInstanceOf[UTF8String]
    val bigNumeric = new BigNumeric(new BigDecimal(utf8str.toString))
    System.out.println("deserialize:   " + utf8str.toString)
    bigNumeric
  }

  override def userClass: Class[BigNumeric] = classOf[BigNumeric]
}

@SQLUserDefinedType(udt = classOf[BigNumericUDT])
class BigNumeric(val number: BigDecimal){
}
