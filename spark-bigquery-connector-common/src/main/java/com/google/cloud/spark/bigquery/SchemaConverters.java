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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import scala.annotation.meta.field;

public class SchemaConverters {

  static final DecimalType NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(
          BigQueryUtil.DEFAULT_NUMERIC_PRECISION, BigQueryUtil.DEFAULT_NUMERIC_SCALE);
  // The maximum nesting depth of a BigQuery RECORD:
  static final int MAX_BIGQUERY_NESTED_DEPTH = 15;
  // Numeric cannot have more than 29 digits left of the dot. For more details
  // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#parameterized_decimal_type
  private static final int NUMERIC_MAX_LEFT_OF_DOT_DIGITS = 29;

  private final SchemaConvertersConfiguration configuration;

  private SchemaConverters(SchemaConvertersConfiguration configuration) {
    this.configuration = configuration;
  }

  public static SchemaConverters from(SchemaConvertersConfiguration configuration) {
    return new SchemaConverters(configuration);
  }

  /** Convert a BigQuery schema to a Spark schema */
  public StructType toSpark(Schema schema) {
    List<StructField> fieldList =
        schema.getFields().stream().map(this::convert).collect(Collectors.toList());
    StructType structType = new StructType(fieldList.toArray(new StructField[0]));

    return structType;
  }

  /**
   * Retrieves and returns BigQuery Schema from TableInfo. If the table support pseudo columns, they
   * are added to schema before schema is returned to the caller.
   */
  public Schema getSchemaWithPseudoColumns(TableInfo tableInfo) {
    TimePartitioning timePartitioning = null;
    TableDefinition tableDefinition = tableInfo.getDefinition();
    if (tableDefinition instanceof StandardTableDefinition) {
      timePartitioning = ((StandardTableDefinition) tableDefinition).getTimePartitioning();
    }
    boolean tableSupportsPseudoColumns =
        timePartitioning != null
            && timePartitioning.getField() == null
            && timePartitioning.getType() != null;

    Schema schema = tableDefinition.getSchema();
    if (tableSupportsPseudoColumns) {
      ArrayList<Field> fields = new ArrayList<Field>(schema.getFields());
      fields.add(
          createBigQueryFieldBuilder(
                  "_PARTITIONTIME", LegacySQLTypeName.TIMESTAMP, Field.Mode.NULLABLE, null)
              .build());
      // Issue #748: _PARTITIONDATE exists only when partition type is day (not hour/month/year)
      if (timePartitioning.getType().equals(Type.DAY)) {
        fields.add(
            createBigQueryFieldBuilder(
                    "_PARTITIONDATE", LegacySQLTypeName.DATE, Field.Mode.NULLABLE, null)
                .build());
      }
      schema = Schema.of(fields);
    }
    return schema;
  }

  public InternalRow convertToInternalRow(
      Schema schema,
      List<String> namesInOrder,
      GenericRecord record,
      Optional<StructType> userProvidedSchema) {
    List<StructField> userProvidedFieldList =
        Arrays.stream(userProvidedSchema.orElse(new StructType()).fields())
            .collect(Collectors.toList());

    return convertAll(schema.getFields(), record, namesInOrder, userProvidedFieldList);
  }

  Object convert(Field field, Object value, StructField userProvidedField) {
    if (value == null) {
      return null;
    }

    if (field.getMode() == Field.Mode.REPEATED) {
      // rather than recurring down we strip off the repeated mode
      // Due to serialization issues, reconstruct the type using reflection:
      // See: https://github.com/googleapis/google-cloud-java/issues/3942
      LegacySQLTypeName fType = LegacySQLTypeName.valueOfStrict(field.getType().name());
      Field nestedField =
          Field.newBuilder(field.getName(), fType, field.getSubFields())
              // As long as this is not repeated it works, but technically arrays cannot contain
              // nulls, so select required instead of nullable.
              .setMode(Field.Mode.REQUIRED)
              .build();

      List<Object> valueList = (List<Object>) value;
      return new GenericArrayData(
          valueList.stream()
              .map(v -> convert(nestedField, v, getStructFieldForRepeatedMode(userProvidedField)))
              .collect(Collectors.toList()));
    }

    Object datum = convertByBigQueryType(field, value, userProvidedField);
    Optional<Object> customDatum =
        getCustomDataType(field).map(dt -> ((UserDefinedType) dt).deserialize(datum));
    return customDatum.orElse(datum);
  }

  private StructField getStructFieldForRepeatedMode(StructField field) {
    StructField nestedField = null;

    if (field != null) {
      ArrayType arrayType = ((ArrayType) field.dataType());
      nestedField =
          new StructField(
              field.name(),
              arrayType.elementType(),
              arrayType.containsNull(),
              Metadata.empty()); // safe to pass empty metadata as it is not used anywhere
    }
    return nestedField;
  }

  Object convertByBigQueryType(Field bqField, Object value, StructField userProvidedField) {
    if (LegacySQLTypeName.INTEGER.equals(bqField.getType())
        || LegacySQLTypeName.FLOAT.equals(bqField.getType())
        || LegacySQLTypeName.BOOLEAN.equals(bqField.getType())
        || LegacySQLTypeName.DATE.equals(bqField.getType())
        || LegacySQLTypeName.TIME.equals(bqField.getType())
        || LegacySQLTypeName.TIMESTAMP.equals(bqField.getType())) {
      return value;
    }

    if (LegacySQLTypeName.STRING.equals(bqField.getType())
        || LegacySQLTypeName.DATETIME.equals(bqField.getType())
        || LegacySQLTypeName.GEOGRAPHY.equals(bqField.getType())
        || LegacySQLTypeName.JSON.equals(bqField.getType())) {
      return UTF8String.fromBytes(((Utf8) value).getBytes());
    }

    if (LegacySQLTypeName.BYTES.equals(bqField.getType())) {
      return getBytes((ByteBuffer) value);
    }

    if (LegacySQLTypeName.NUMERIC.equals(bqField.getType())
        || LegacySQLTypeName.BIGNUMERIC.equals(bqField.getType())) {
      byte[] bytes = getBytes((ByteBuffer) value);
      int scale = bqField.getScale().intValue();
      BigDecimal b = new BigDecimal(new BigInteger(bytes), scale);
      Decimal d = Decimal.apply(b, bqField.getPrecision().intValue(), scale);

      return d;
    }

    if (LegacySQLTypeName.RECORD.equals(bqField.getType())) {
      List<String> namesInOrder = null;
      List<StructField> structList = null;

      if (userProvidedField != null) {
        structList =
            Arrays.stream(((StructType) userProvidedField.dataType()).fields())
                .collect(Collectors.toList());

        namesInOrder = structList.stream().map(StructField::name).collect(Collectors.toList());
      } else {
        namesInOrder =
            bqField.getSubFields().stream().map(Field::getName).collect(Collectors.toList());
      }

      return convertAll(bqField.getSubFields(), (GenericRecord) value, namesInOrder, structList);
    }

    throw new IllegalStateException("Unexpected type: " + bqField.getType());
  }

  private byte[] getBytes(ByteBuffer buf) {
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);

    return bytes;
  }

  // Schema is not recursive so add helper for sequence of fields
  GenericInternalRow convertAll(
      FieldList fieldList,
      GenericRecord record,
      List<String> namesInOrder,
      List<StructField> userProvidedFieldList) {
    Map<String, Object> fieldMap = new HashMap<>();

    Map<String, StructField> userProvidedFieldMap =
        userProvidedFieldList == null
            ? new HashMap<>()
            : userProvidedFieldList.stream()
                .collect(Collectors.toMap(StructField::name, Function.identity()));

    fieldList.stream()
        .forEach(
            field ->
                fieldMap.put(
                    field.getName(),
                    convert(
                        field,
                        record.get(field.getName()),
                        userProvidedFieldMap.get(field.getName()))));

    Object[] values = new Object[namesInOrder.size()];
    for (int i = 0; i < namesInOrder.size(); i++) {
      values[i] = fieldMap.get(namesInOrder.get(i));
    }

    return new GenericInternalRow(values);
  }

  /**
   * Create a function that converts an Avro row with the given BigQuery schema to a Spark SQL row
   *
   * <p>The conversion is based on the BigQuery schema, not Avro Schema, because the Avro schema is
   * very painful to use.
   *
   * <p>Not guaranteed to be stable across all versions of Spark.
   */
  @VisibleForTesting
  StructField convert(Field field) {
    DataType dataType = getDataType(field);
    boolean nullable = true;

    if (field.getMode() == Field.Mode.REQUIRED) {
      nullable = false;
    } else if (field.getMode() == Field.Mode.REPEATED) {
      dataType = new ArrayType(dataType, true);
    }

    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (field.getDescription() != null) {
      metadataBuilder.putString("description", field.getDescription());
      metadataBuilder.putString("comment", field.getDescription());
    }
    // JSON
    if (LegacySQLTypeName.JSON.equals(field.getType())) {
      metadataBuilder.putString("sqlType", "JSON");
    }

    Metadata metadata = metadataBuilder.build();
    return convertMap(field, metadata) //
        .orElse(new StructField(field.getName(), dataType, nullable, metadata));
  }

  Optional<StructField> convertMap(Field field, Metadata metadata) {
    if (field.getMode() != Field.Mode.REPEATED) {
      return Optional.empty();
    }
    if (field.getType() != LegacySQLTypeName.RECORD) {
      return Optional.empty();
    }
    FieldList subFields = field.getSubFields();
    if (subFields.size() != 2) {
      return Optional.empty();
    }
    Set<String> subFieldNames = subFields.stream().map(Field::getName).collect(Collectors.toSet());
    if (!subFieldNames.contains("key") || !subFieldNames.contains("value")) {
      // no "key" or "value" fields
      return Optional.empty();
    }
    Field key = subFields.get("key");
    Field value = subFields.get("value");
    MapType mapType = DataTypes.createMapType(convert(key).dataType(), convert(value).dataType());
    // The returned type is not nullable because the original field is a REPEATED, not NULLABLE.
    // There are some compromises we need to do as BigQuery has no native MAP type
    return Optional.of(new StructField(field.getName(), mapType, /* nullable */ false, metadata));
  }

  private DataType getDataType(Field field) {
    return getCustomDataType(field).orElseGet(() -> getStandardDataType(field));
  }

  @VisibleForTesting
  Optional<DataType> getCustomDataType(Field field) {
    // metadata is kept in the description
    String description = field.getDescription();
    if (description != null) {
      // All supported types are serialized to records
      if (LegacySQLTypeName.RECORD.equals(field.getType())) {
        // we don't have many types, so we keep parsing to minimum
        return SupportedCustomDataType.forDescription(description)
            .map(SupportedCustomDataType::getSparkDataType);
      }
    }
    return Optional.empty();
  }

  private DataType getStandardDataType(Field field) {
    if (LegacySQLTypeName.INTEGER.equals(field.getType())) {
      return DataTypes.LongType;
    } else if (LegacySQLTypeName.FLOAT.equals(field.getType())) {
      return DataTypes.DoubleType;
    } else if (LegacySQLTypeName.NUMERIC.equals(field.getType())) {
      int precision =
          Optional.ofNullable(field.getPrecision())
              .map(Long::intValue)
              .orElse(BigQueryUtil.DEFAULT_NUMERIC_PRECISION);
      int scale =
          Optional.ofNullable(field.getScale())
              .map(Long::intValue)
              .orElse(BigQueryUtil.DEFAULT_NUMERIC_SCALE);
      return DataTypes.createDecimalType(precision, scale);
    } else if (LegacySQLTypeName.BIGNUMERIC.equals(field.getType())) {
      int precision =
          Optional.ofNullable(field.getPrecision())
              .map(Long::intValue)
              .orElse(BigQueryUtil.DEFAULT_BIG_NUMERIC_PRECISION);
      if (precision > DecimalType.MAX_PRECISION()) {
        throw new IllegalArgumentException(
            String.format(
                "BigNumeric precision is too wide (%d), Spark can only handle decimal types with max precision of %d",
                precision, DecimalType.MAX_PRECISION()));
      }
      int scale =
          Optional.ofNullable(field.getScale())
              .map(Long::intValue)
              .orElse(BigQueryUtil.DEFAULT_BIG_NUMERIC_SCALE);
      return DataTypes.createDecimalType(precision, scale);
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
      List<StructField> structFields =
          field.getSubFields().stream().map(this::convert).collect(Collectors.toList());
      return new StructType(structFields.toArray(new StructField[0]));
    } else if (LegacySQLTypeName.GEOGRAPHY.equals(field.getType())) {
      return DataTypes.StringType;
    } else if (LegacySQLTypeName.JSON.equals(field.getType())) {
      return DataTypes.StringType;
    } else {
      throw new IllegalStateException("Unexpected type: " + field.getType());
    }
  }

  /** Spark ==> BigQuery Schema Converter utils: */
  public Schema toBigQuerySchema(StructType sparkSchema) {
    FieldList bigQueryFields = sparkToBigQueryFields(sparkSchema, 0);
    return Schema.of(bigQueryFields);
  }

  /**
   * Returns a FieldList of all the Spark StructField objects, converted to BigQuery Field objects
   */
  private FieldList sparkToBigQueryFields(StructType sparkStruct, int depth) {
    Preconditions.checkArgument(
        depth < MAX_BIGQUERY_NESTED_DEPTH, "Spark Schema exceeds BigQuery maximum nesting depth.");
    List<Field> bqFields = new ArrayList<>();
    for (StructField field : sparkStruct.fields()) {
      bqFields.add(createBigQueryColumn(field, depth));
    }
    return FieldList.of(bqFields);
  }

  /** Converts a single StructField to a BigQuery Field (column). */
  @VisibleForTesting
  protected Field createBigQueryColumn(StructField sparkField, int depth) {
    DataType sparkType = sparkField.dataType();
    String fieldName = sparkField.name();
    Field.Mode fieldMode = (sparkField.nullable()) ? Field.Mode.NULLABLE : Field.Mode.REQUIRED;
    FieldList subFields = null;
    LegacySQLTypeName fieldType;
    OptionalLong scale = OptionalLong.empty();
    long precision = 0;

    if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;

      fieldMode = Field.Mode.REPEATED;
      sparkType = arrayType.elementType();
    }

    if (sparkType instanceof StructType) {
      subFields = sparkToBigQueryFields((StructType) sparkType, depth + 1);
      fieldType = LegacySQLTypeName.RECORD;
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      fieldMode = Field.Mode.REPEATED;
      fieldType = LegacySQLTypeName.RECORD;
      subFields =
          FieldList.of(
              Field.newBuilder("key", toBigQueryType(mapType.keyType(), sparkField.metadata()))
                  .setMode(Mode.REQUIRED)
                  .build(),
              Field.newBuilder("value", toBigQueryType(mapType.valueType(), sparkField.metadata()))
                  .setMode(mapType.valueContainsNull() ? Mode.NULLABLE : Mode.REQUIRED)
                  .build());
    } else if (sparkType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkType;
      int leftOfDotDigits = decimalType.precision() - decimalType.scale();
      fieldType =
          (decimalType.scale() > BigQueryUtil.DEFAULT_NUMERIC_SCALE
                  || leftOfDotDigits > NUMERIC_MAX_LEFT_OF_DOT_DIGITS)
              ? LegacySQLTypeName.BIGNUMERIC
              : LegacySQLTypeName.NUMERIC;
      scale = OptionalLong.of(decimalType.scale());
      precision = decimalType.precision();
    } else {
      fieldType = toBigQueryType(sparkType, sparkField.metadata());
    }

    Field.Builder fieldBuilder =
        createBigQueryFieldBuilder(fieldName, fieldType, fieldMode, subFields);
    Optional<String> description = getDescriptionOrCommentOfField(sparkField);

    if (description.isPresent()) {
      fieldBuilder.setDescription(description.get());
    }

    // if this is a decimal type
    if (scale.isPresent()) {
      fieldBuilder.setPrecision(precision);
      fieldBuilder.setScale(scale.getAsLong());
    }

    return fieldBuilder.build();
  }

  public static Optional<String> getDescriptionOrCommentOfField(StructField field) {
    if (!field.getComment().isEmpty()) {
      return Optional.of(field.getComment().get());
    }
    if (field.metadata().contains("description")
        && field.metadata().getString("description") != null) {
      return Optional.of(field.metadata().getString("description"));
    }
    return Optional.empty();
  }

  @VisibleForTesting
  protected LegacySQLTypeName toBigQueryType(DataType elementType, Metadata metadata) {
    if (elementType instanceof BinaryType) {
      return LegacySQLTypeName.BYTES;
    }
    if (elementType instanceof ByteType
        || elementType instanceof ShortType
        || elementType instanceof IntegerType
        || elementType instanceof LongType) {
      return LegacySQLTypeName.INTEGER;
    }
    if (elementType instanceof BooleanType) {
      return LegacySQLTypeName.BOOLEAN;
    }
    if (elementType instanceof FloatType || elementType instanceof DoubleType) {
      return LegacySQLTypeName.FLOAT;
    }
    if (elementType instanceof StringType) {
      if (SparkBigQueryUtil.isJson(metadata)) {
        return LegacySQLTypeName.JSON;
      }
      return LegacySQLTypeName.STRING;
    }
    if (elementType instanceof TimestampType) {
      return LegacySQLTypeName.TIMESTAMP;
    }
    if (elementType instanceof DateType) {
      return LegacySQLTypeName.DATE;
    }
    throw new IllegalArgumentException("Data type not expected: " + elementType.simpleString());
  }

  private Field.Builder createBigQueryFieldBuilder(
      String name, LegacySQLTypeName type, Field.Mode mode, FieldList subFields) {
    return Field.newBuilder(name, type, subFields).setMode(mode);
  }
}
