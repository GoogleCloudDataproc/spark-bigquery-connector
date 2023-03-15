/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.spark.sql.types.*;

import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.arrow.vector.types.pojo.Field;

/**
 * ArrowSchemaConverter class for accessing values and converting
 * arrow data types to the types supported by big query.
 */
public abstract class ArrowSchemaConverter extends ColumnVector {
  public abstract ValueVector vector();
  @Override
  public boolean hasNull() {
    return vector().getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return vector().getNullCount();
  }

  @Override
  public void close() {
    vector().close();
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnVector getChild(int ordinal) { throw new UnsupportedOperationException(); }

  private static DataType fromArrowType(ArrowType arrowType)
  {
    switch (arrowType.getTypeID())
    {
      case Int: return DataTypes.LongType;
      case Bool: return DataTypes.BooleanType;
      case FloatingPoint: return DataTypes.DoubleType;
      case Binary: return DataTypes.BinaryType;
      case Utf8: return DataTypes.StringType;
      case Date: return DataTypes.DateType;
      case Time:
      case Timestamp: return DataTypes.TimestampType;
      case Decimal: return DataTypes.createDecimalType();
    }

    throw new UnsupportedOperationException("Unsupported data type " + arrowType.toString());
  }

  private static DataType fromArrowField(Field field)
  {
    if (field.getType().getTypeID() == ArrowTypeID.List)
    {
      Field elementField = field.getChildren().get(0);
      DataType elementType = fromArrowField(elementField);

      return new ArrayType(elementType, elementField.isNullable());
    }

    if (field.getType().getTypeID() == ArrowTypeID.Struct)
    {
      java.util.List<Field> fieldChildren = field.getChildren();
      StructField[] structFields = new StructField[fieldChildren.size()];

      int ind = 0;

      for (Field childField : field.getChildren())
      {
        DataType childType = fromArrowField(childField);
        structFields[ind++] = new StructField(childField.getName(), childType, childField.isNullable(), Metadata.empty());
      }

      return new StructType(structFields);
    }

    return fromArrowType(field.getType());
  }

  ArrowSchemaConverter(ValueVector vector) {
    super(fromArrowField(vector.getField()));
  }

  public static ArrowSchemaConverter newArrowSchemaConverter(ValueVector vector, StructField userProvidedField) {
    if (vector instanceof BitVector) {
      return new ArrowSchemaConverter.BooleanAccessor((BitVector) vector);
    } else if (vector instanceof BigIntVector) {
      return new ArrowSchemaConverter.LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float8Vector) {
      return new ArrowSchemaConverter.DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      return new ArrowSchemaConverter.DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof Decimal256Vector) {
      return new ArrowSchemaConverter.Decimal256Accessor((Decimal256Vector) vector);
    } else if (vector instanceof VarCharVector) {
      return new ArrowSchemaConverter.StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return new ArrowSchemaConverter.BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new ArrowSchemaConverter.DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeMicroVector) {
      return new ArrowSchemaConverter.TimeMicroVectorAccessor((TimeMicroVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      return new ArrowSchemaConverter.TimestampMicroVectorAccessor((TimeStampMicroVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      return new ArrowSchemaConverter.TimestampMicroTZVectorAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      return new ArrowSchemaConverter.ArrayAccessor(listVector, userProvidedField);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      return new ArrowSchemaConverter.StructAccessor(structVector, userProvidedField);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowSchemaConverter {
    private final BitVector vector;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final boolean getBoolean(int rowId) {
      return vector.get(rowId) == 1;
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }
  }

  private static class LongAccessor extends ArrowSchemaConverter {
    private final BigIntVector vector;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final byte getByte(int rowId) {
      return (byte)getLong(rowId);
    }

    @Override
    public final short getShort(int rowId) {
      return (short)getLong(rowId);
    }

    @Override
    public final int getInt(int rowId) {
      return (int)getLong(rowId);
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  private static class DoubleAccessor extends ArrowSchemaConverter {
    private final Float8Vector vector;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final double getDouble(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  private static class DecimalAccessor extends ArrowSchemaConverter {
    private final DecimalVector vector;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.vector = vector;
    }

    // Implemented this method for tpc-ds queries that cast from Decimal to Byte
    @Override
    public byte getByte(int rowId) {
      return vector.getObject(rowId).byteValueExact();
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(((DecimalVector)vector).getObject(rowId), precision, scale);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  private static class Decimal256Accessor extends ArrowSchemaConverter {
    private final Decimal256Vector vector;

    Decimal256Accessor(Decimal256Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }


    public UTF8String getUTF8String(int rowId){
      if (isNullAt(rowId)) {
        return null;
      }

      BigDecimal bigDecimal = ((Decimal256Vector)vector).getObject(rowId);
      return UTF8String.fromString(bigDecimal.toPlainString());
    }

    // Implemented this method for reading BigNumeric values
    @Override
    public final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(((Decimal256Vector)vector).getObject(rowId), precision, scale);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }
  }

  private static class StringAccessor extends ArrowSchemaConverter {
    private final VarCharVector vector;
    StringAccessor(VarCharVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final UTF8String getUTF8String(int rowId) {
      if (vector.isSet(rowId) == 0) {
        return null;
      } else {
        ArrowBuf offsets = ((VarCharVector)vector).getOffsetBuffer();
        int index = rowId * VarCharVector.OFFSET_WIDTH;
        int start = offsets.getInt(index);
        int end = offsets.getInt(index + VarCharVector.OFFSET_WIDTH);

        /* Since the result is accessed lazily if the memory address is corrupted we
         * might lose the data. Might be better to include a byte array. Not doing so
         * for performance reasons.
         */
        return UTF8String.fromAddress(/* base = */null,
                ((VarCharVector)vector).getDataBuffer().memoryAddress() + start,
            end - start);
      }
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }
  }

  private static class BinaryAccessor extends ArrowSchemaConverter {
    private final VarBinaryVector vector;
    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final byte[] getBinary(int rowId) {
      return vector.getObject(rowId);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  private static class DateAccessor extends ArrowSchemaConverter { ;
    private final DateDayVector vector;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    /**
     * Interpreting Data here as int to keep it consistent with Avro.
     */
    @Override
    public final int getInt(int rowId) {
      return ((DateDayVector)vector).get(rowId);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  private static class TimeMicroVectorAccessor extends ArrowSchemaConverter {
    private final TimeMicroVector vector;

    TimeMicroVectorAccessor(TimeMicroVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }


  private static class TimestampMicroVectorAccessor extends ArrowSchemaConverter {
    private static final int ONE_THOUSAND = 1_000;
    private static final int ONE_MILLION = 1_000_000;
    private static final int ONE_BILLION = 1_000_000_000;

    private final TimeStampMicroVector vector;

    TimestampMicroVectorAccessor(TimeStampMicroVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }


    @Override
    public final UTF8String getUTF8String(int rowId) {
      if (isNullAt(rowId)) {
         return null;
      }
      long epoch = getLong(rowId);
      long seconds = epoch / ONE_MILLION;
      int nanoOfSeconds = (int)(epoch % ONE_MILLION) * ONE_THOUSAND;

      // The method LocalDateTime.ofEpochSecond expects
      // seconds as the number of seconds from the epoch of 1970-01-01T00:00:00Z and
      // the nanosecond within the second, from 0 to 999,999,999.
      // For time prior to 1970-01-01, seconds and nanoseconds both could be negative, but
      // negative nanoseconds is not allowed, hence for such cases adding 1_000_000_000 to
      // nanosecond's value and subtracting 1 from the second's value.
      if(nanoOfSeconds < 0) {
        seconds--;
        nanoOfSeconds += ONE_BILLION;
      }

      LocalDateTime dateTime = LocalDateTime.ofEpochSecond(seconds, nanoOfSeconds, ZoneOffset.UTC);
      return UTF8String.fromString(dateTime.toString());
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  private static class TimestampMicroTZVectorAccessor extends ArrowSchemaConverter {
    private final TimeStampMicroTZVector vector;

    TimestampMicroTZVectorAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }


    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }
  }

  private static class ArrayAccessor extends ArrowSchemaConverter {
    private final ListVector vector;

    private final ArrowSchemaConverter arrayData;

    ArrayAccessor(ListVector vector, StructField userProvidedField) {
      super(vector);
      this.vector = vector;
      StructField structField = null;

      // this is to support Array of StructType/StructVector
      if(userProvidedField != null) {
        DataType dataType = userProvidedField.dataType();
        ArrayType arrayType = dataType instanceof MapType ? convertMapTypeToArrayType((MapType) dataType) : (ArrayType) dataType;
        structField =
            new StructField(
                vector.getDataVector().getName(),
                arrayType.elementType(),
                arrayType.containsNull(),
                Metadata.empty());// safe to pass empty metadata as it is not used anywhere
      }

      this.arrayData = newArrowSchemaConverter(vector.getDataVector(), structField);
    }


    static ArrayType convertMapTypeToArrayType(MapType mapType) {
      StructField key = StructField.apply("key", mapType.keyType(), false, Metadata.empty());
      StructField value = StructField.apply("value", mapType.valueType(), mapType.valueContainsNull(), Metadata.empty());
      StructField[] fields = new StructField[] { key, value};
      return ArrayType.apply(new StructType(fields));
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }


    @Override
    public final ColumnarArray getArray(int rowId) {
      ArrowBuf offsets = ((ListVector)vector).getOffsetBuffer();
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = offsets.getInt(index);
      int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
      return new ColumnarArray(arrayData, start, end - start);
    }

    @Override
    public ColumnarMap getMap(int rowId) {
      ArrowBuf offsets = ((ListVector)vector).getOffsetBuffer();
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = offsets.getInt(index);
      int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
      ColumnVector keys = ((StructAccessor)arrayData).childColumns[0];
      ColumnVector values = ((StructAccessor)arrayData).childColumns[1];
      return new ColumnarMap(keys, values, start, end - start);
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }

  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   */
  private static class StructAccessor extends ArrowSchemaConverter {
    private final StructVector vector;
    private ArrowSchemaConverter childColumns[];

    StructAccessor(StructVector structVector, StructField userProvidedField) {
      super(structVector);
      this.vector = structVector;
      if(userProvidedField !=null) {
        List<StructField> structList =
              Arrays.stream(((StructType) userProvidedField.dataType()).fields())
                    .collect(Collectors.toList());

        this.childColumns = new ArrowSchemaConverter[structList.size()];

        Map<String, ValueVector> valueVectorMap =
                structVector
                        .getChildrenFromFields()
                        .stream()
                        .collect(Collectors.toMap(ValueVector::getName, valueVector -> valueVector));

        for (int i = 0; i < childColumns.length; ++i) {
          StructField structField = structList.get(i);
          childColumns[i] =
                  newArrowSchemaConverter(valueVectorMap.get(structField.name()), structField);
        }
      } else {
        childColumns = new ArrowSchemaConverter[structVector.size()];
        for (int i = 0; i < childColumns.length; ++i) {
          childColumns[i] = newArrowSchemaConverter(structVector.getVectorById(i), /*userProvidedField=*/null);
        }
      }
    }

    @Override
    public final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }


    @Override
    public ColumnVector getChild(int ordinal) { return childColumns[ordinal]; }


    @Override
    public void close() {
      if (childColumns != null) {
        for (ColumnVector v : childColumns) {
          v.close();
        }
        childColumns = null;
      }
      vector.close();
    }

    @Override
    public final ValueVector vector() {
      return vector;
    }
  }
}
