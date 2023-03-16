package org.apache.spark.bigquery;

import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;

public class BigNumericUDT extends UserDefinedType<BigNumeric> {

  @Override
  public DataType sqlType() {
    return DataTypes.BinaryType;
  }

  @Override
  public byte[] serialize(BigNumeric obj) {
    return BigDecimalByteStringEncoder.encodeToBigNumericByteString(obj.getNumber()).toByteArray();
  }

  @Override
  public BigNumeric deserialize(Object datum) {
    if (!(datum instanceof byte[])) {
      throw new IllegalArgumentException(
          "Failed to deserialize, was expecting an instance of byte[], "
              + "instead got an instance of "
              + datum.getClass());
    }

    byte[] byteArr = (byte[]) datum;
    BigDecimal bigDecimal =
        BigDecimalByteStringEncoder.decodeBigNumericByteString(ByteString.copyFrom(byteArr));
    return new BigNumeric(bigDecimal);
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
