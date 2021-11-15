package org.apache.spark.bigquery;

import java.math.BigDecimal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;

public class BigNumericUDT extends UserDefinedType<BigNumeric> {

  @Override
  public DataType sqlType() {
    return DataTypes.StringType;
  }

  @Override
  public UTF8String serialize(BigNumeric obj) {
    String number = obj.getNumber().toPlainString();
    return UTF8String.fromString(number);
  }

  @Override
  public BigNumeric deserialize(Object datum) {
    if (!(datum instanceof UTF8String)) {
      throw new IllegalArgumentException(
          "Failed to deserialize, was expecting an instance of UTF8String, "
              + "instead got an instance of "
              + datum.getClass());
    }

    UTF8String utf8str = (UTF8String) datum;
    BigNumeric bigNumeric = new BigNumeric(new BigDecimal(utf8str.toString()));
    return bigNumeric;
  }

  @Override
  public Class<BigNumeric> userClass() {
    return BigNumeric.class;
  }

  @Override
  public String pyUDT() {
    return "google.cloud.spark.bigquery.big_numeric_support.BigNumericUDT";
  }
}
