package org.apache.spark.bigquery;

import org.apache.spark.sql.types.SQLUserDefinedType;

import java.io.Serializable;
import java.math.BigDecimal;

@SQLUserDefinedType(udt = BigNumericUDT.class)
public class BigNumeric implements Serializable {
    private static final long serialVersionUID = -2206218441920461328L;
    private final BigDecimal number;

    public BigNumeric(BigDecimal number) {
        this.number = number;
    }

    public BigDecimal getNumber() {
        return this.number;
    }

    @Override
    public String toString() {
        return this.number != null ? this.number.toPlainString() : "";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BigNumeric)) {
            return false;
        }

        BigNumeric otherBigNumeric = (BigNumeric) other;
        return this.number != null ? this.number.equals(otherBigNumeric.number) : otherBigNumeric.number == null;
    }

    @Override
    public int hashCode() {
       return this.number != null ? this.number.hashCode() : 0;
    }
}
