package com.google.cloud.spark.bigquery;

import org.apache.parquet.schema.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;

import java.util.stream.Stream;

public class ParquetSchemaConverter {

    public static MessageType sparkSchemaToParquet(StructType sparkSchema) {

      Type[] fields =
          Stream.of(sparkSchema.fields())
              .map(ParquetSchemaConverter::sparkFieldToParquetType)
              .toArray(Type[]::new);
      return Types.buildMessage().addFields(fields).named("spark");
    }

    static Type sparkFieldToParquetType(StructField field) {
      Type.Repetition repetition =
          field.nullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
      return sparkFieldToParquetType(field, repetition);
    }

    private static Type sparkFieldToParquetType(StructField field, Type.Repetition repetition) {
      DataType dataType = field.dataType();
      String name = field.name();

      if (dataType instanceof BinaryType) {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name);
      }
      if (dataType instanceof ByteType
          || dataType instanceof ShortType
          || dataType instanceof IntegerType
          || dataType instanceof LongType) {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name);
      }
      if (dataType instanceof BooleanType) {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name);
      }
      if (dataType instanceof FloatType || dataType instanceof DoubleType) {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name);
      }
      if (dataType instanceof DecimalType) {
        DecimalType decimalType = (DecimalType) dataType;
        if (decimalType.precision() <= SchemaConverters.BQ_NUMERIC_PRECISION
            && decimalType.scale() <= SchemaConverters.BQ_NUMERIC_SCALE) {
          int precision = ((DecimalType) dataType).precision();
          int scale = ((DecimalType) dataType).scale();
          PrimitiveType.PrimitiveTypeName typeName =
              PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
          int length = Decimal.minBytesForPrecision()[precision];
          if (precision <= Decimal.MAX_INT_DIGITS()) {
            typeName = PrimitiveType.PrimitiveTypeName.INT32;
            length = Decimal.minBytesForPrecision()[Decimal.MAX_INT_DIGITS()];
          } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
            typeName = PrimitiveType.PrimitiveTypeName.INT64;
            length = Decimal.minBytesForPrecision()[Decimal.MAX_LONG_DIGITS()];
          }
          return Types.primitive(typeName, repetition)
              .as(OriginalType.DECIMAL)
              .precision(precision)
              .scale(scale)
              .length(length)
              .named(name);
        } else {
          throw new IllegalArgumentException(
              "Decimal type is too wide to fit in BigQuery Numeric format");
        }
      }
      if (dataType instanceof StringType) {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
            .as(OriginalType.UTF8)
            .named(name);
      }
      if (dataType instanceof TimestampType) {
        // return builder.TIMESTAMP; FIXME: Restore this correct conversion when the Vortex
        // team adds microsecond support to their backend
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .as(OriginalType.TIMESTAMP_MICROS)
            .named(name);
      }
      if (dataType instanceof DateType) {
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(OriginalType.DATE)
            .named(name);
      }
      if (dataType instanceof ArrayType) {
        StructField elementField =
            StructField.apply(
                name,
                ((ArrayType) dataType).elementType(),
                ((ArrayType) dataType).containsNull(),
                field.metadata());
        Type elementType = sparkFieldToParquetType(elementField, Type.Repetition.REPEATED);
        return elementType;
      }
      if(dataType instanceof StructType) {
        StructField[] sparkFields = ((StructType)dataType).fields();
        Type[] parquetFields =
   Stream.of(sparkFields).map(ParquetSchemaConverter::sparkFieldToParquetType).toArray(Type[]::new);
        return Types.buildGroup(repetition).addFields(parquetFields).named(name);
      }
      if (dataType instanceof UserDefinedType) {
        DataType userDefinedType = ((UserDefinedType) dataType).sqlType();
        StructField wrappedField =
            StructField.apply(name, userDefinedType, field.nullable(), field.metadata());
        return sparkFieldToParquetType(wrappedField);
      }
      if (dataType instanceof MapType) {
        throw new IllegalArgumentException(SchemaConverters.MAPTYPE_ERROR_MESSAGE);
      } else {
        throw new IllegalArgumentException("Data type not expected: " + dataType.simpleString());
      }
    }

    void convertRow(StructType sparkSchema, MessageType parquetSchema, InternalRow row) {
      parquetSchema.
    }


}
