package com.google.cloud.spark.bigquery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class SchemaConverters {
    // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
    private final static int BQ_NUMERIC_PRECISION = 38;
    private final static int BQ_NUMERIC_SCALE = 9;
    private final static DecimalType NUMERIC_SPARK_TYPE = DataTypes.createDecimalType(
    BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);

    /** Convert a BigQuery schema to a Spark schema */
    public static StructType toSpark(Schema schema) {
        List<StructField> fieldList = schema.getFields().stream()
                .map(SchemaConverters::convert).collect(Collectors.toList());
        StructType structType = new StructType(fieldList.toArray(new StructField[0]));

        return structType;
    }

    public static InternalRow createRowConverter(Schema schema, List<String> namesInOrder, GenericRecord record) {
        return convertAll(schema.getFields(), record, namesInOrder);
    }

    static Object convert(Field field, Object value) {
        if (value == null) {
            return null;
        }

        if (field.getMode() == Field.Mode.REPEATED) {
            // rather than recurring down we strip off the repeated mode
            // Due to serialization issues, reconstruct the type using reflection:
            // See: https://github.com/googleapis/google-cloud-java/issues/3942
            LegacySQLTypeName fType = LegacySQLTypeName.valueOfStrict(field.getType().name());
            Field nestedField = Field.newBuilder(field.getName(), fType, field.getSubFields())
                    // As long as this is not repeated it works, but technically arrays cannot contain
                    // nulls, so select required instead of nullable.
                    .setMode(Field.Mode.REQUIRED)
                    .build();

            List<Object> valueList = (List<Object>) value;

            return new GenericArrayData(valueList.stream().map(v -> convert(nestedField, v)).collect(Collectors.toList()));
        }

        if (LegacySQLTypeName.INTEGER.equals(field.getType()) ||
                LegacySQLTypeName.FLOAT.equals(field.getType()) ||
                LegacySQLTypeName.BOOLEAN.equals(field.getType()) ||
                LegacySQLTypeName.DATE.equals(field.getType()) ||
                LegacySQLTypeName.TIME.equals(field.getType()) ||
                LegacySQLTypeName.TIMESTAMP.equals(field.getType())) {
            return value;
        }

        if (LegacySQLTypeName.STRING.equals(field.getType()) ||
                LegacySQLTypeName.DATETIME.equals(field.getType()) ||
                LegacySQLTypeName.GEOGRAPHY.equals(field.getType())) {
            return UTF8String.fromBytes(((Utf8)value).getBytes());
        }

        if (LegacySQLTypeName.BYTES.equals(field.getType())) {
            return getBytes((ByteBuffer)value);
        }

        if (LegacySQLTypeName.NUMERIC.equals(field.getType())) {
            byte[] bytes = getBytes((ByteBuffer)value);
            BigDecimal b = new BigDecimal(new BigInteger(bytes), BQ_NUMERIC_SCALE);
            Decimal d = Decimal.apply(b, BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);

            return d;
        }

        if (LegacySQLTypeName.RECORD.equals(field.getType())) {
            return convertAll(field.getSubFields(),
                    (GenericRecord)value,
                    field.getSubFields().stream().map(f -> f.getName()).collect(Collectors.toList()));
        }

        throw new IllegalStateException("Unexpected type: " + field.getType());
    }

    private static byte[] getBytes(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);

        return bytes;
    }

    // Schema is not recursive so add helper for sequence of fields
    static GenericInternalRow convertAll(FieldList fieldList,
                                  GenericRecord record,
                                  List<String> namesInOrder) {


        Map<String, Object> fieldMap = new HashMap<>();

       fieldList.stream().forEach(field ->
               fieldMap.put(field.getName(), convert(field, record.get(field.getName()))));

        Object[] values = new Object[namesInOrder.size()];
        for (int i = 0; i < namesInOrder.size(); i++) {
            values[i] = fieldMap.get(namesInOrder.get(i));
        }

        return new GenericInternalRow(values);
    }

    /**
     * Create a function that converts an Avro row with the given BigQuery schema to a Spark SQL row
     *
     * The conversion is based on the BigQuery schema, not Avro Schema, because the Avro schema is
     * very painful to use.
     *
     * Not guaranteed to be stable across all versions of Spark.
     */

    private static StructField convert(Field field) {
        DataType dataType = getDataType(field);
        boolean nullable = true;

        if (field.getMode() == Field.Mode.REQUIRED) {
            nullable = false;
        } else if (field.getMode() == Field.Mode.REPEATED) {
            dataType = new ArrayType(dataType, true);
        }

        MetadataBuilder metadata = new MetadataBuilder();
        if (field.getDescription() != null) {
            metadata.putString("description", field.getDescription());
        }

        return new StructField(field.getName(), dataType, nullable, metadata.build());
    }

    private static DataType getDataType(Field field) {

        if (LegacySQLTypeName.INTEGER.equals(field.getType())) {
            return DataTypes.LongType;
        } else if (LegacySQLTypeName.FLOAT.equals(field.getType())) {
            return DataTypes.DoubleType;
        } else if (LegacySQLTypeName.NUMERIC.equals(field.getType())) {
            return NUMERIC_SPARK_TYPE;
        } else if (LegacySQLTypeName.STRING.equals(field.getType())) {
            return DataTypes.StringType;
        } else if (LegacySQLTypeName.BOOLEAN.equals(field.getType())) {
            return DataTypes.BooleanType;
        } else if (LegacySQLTypeName.BYTES.equals(field.getType())) {
            return DataTypes.BinaryType;
        } else if (LegacySQLTypeName.DATE.equals(field.getType())) {
            return DataTypes.DateType;
        } else if (LegacySQLTypeName.TIMESTAMP.equals(field.getType())) {
            return DataTypes.TimestampType;
        } else if (LegacySQLTypeName.TIME.equals(field.getType())) {
            return DataTypes.LongType;
            // TODO(#5): add a timezone to allow parsing to timestamp
            // This can be safely cast to TimestampType, but doing so causes the date to be inferred
            // as the current date. It's safer to leave as a stable string and give the user the
            // option of casting themselves.
        } else if (LegacySQLTypeName.DATETIME.equals(field.getType())) {
            return DataTypes.StringType;
        } else if (LegacySQLTypeName.RECORD.equals(field.getType())) {
            List<StructField> structFields = field.getSubFields().stream().map(SchemaConverters::convert).collect(Collectors.toList());
            return new StructType(structFields.toArray(new StructField[0]));
        } else if (LegacySQLTypeName.GEOGRAPHY.equals(field.getType())) {
            return DataTypes.StringType;
        } else {
            throw new IllegalStateException("Unexpected type: " + field.getType());
        }
    }
}
