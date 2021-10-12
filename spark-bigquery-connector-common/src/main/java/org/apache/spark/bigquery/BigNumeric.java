package org.apache.spark.bigquery;

import java.math.BigDecimal;
import org.apache.spark.sql.types.SQLUserDefinedType;

@SQLUserDefinedType(udt = BigNumericUDT.class)
public class BigNumeric {
  private final BigDecimal number;

  public BigNumeric(BigDecimal number) {
    this.number = number;
  }

  public BigDecimal getNumber() {
    return this.number;
  }

  @Override
  public String toString() {
    return this.number.toPlainString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BigNumeric)) {
      return false;
    }

    BigNumeric that = (BigNumeric) o;

    return number != null ? number.equals(that.number) : that.number == null;
  }

  @Override
  public int hashCode() {
    return number != null ? number.hashCode() : 0;
  }
}
