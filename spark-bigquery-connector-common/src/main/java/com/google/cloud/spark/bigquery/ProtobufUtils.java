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
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.bigquery.BigNumericUDT;
import org.apache.spark.bigquery.BigQueryDataTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSqlUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.collection.mutable.IndexedSeq;

public class ProtobufUtils {

  static final Logger logger = LoggerFactory.getLogger(ProtobufUtils.class);
  private static final int BQ_NUMERIC_PRECISION = 38;
  private static final int BQ_NUMERIC_SCALE = 9;
  private static final DecimalType NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);
  // The maximum nesting depth of a BigQuery RECORD:
  private static final int MAX_BIGQUERY_NESTED_DEPTH = 15;
  // For every message, a nested type is name "STRUCT"+i, where i is the
  // number of the corresponding field that is of this type in the containing message.
  private static final String RESERVED_NESTED_TYPE_NAME = "STRUCT";

  private static final LoadingCache<MapType, StructType> MAP_TYPE_STRUCT_TYPE_CACHE =
      CacheBuilder.<MapType, StructType>newBuilder()
          .maximumSize(10_000) // 10,000 map types should be enough for most applications
          .expireAfterWrite(24, TimeUnit.HOURS) // Type definition don't really expire
          .build(
              new CacheLoader<MapType, StructType>() {
                @Override
                public StructType load(MapType key) {
                  return ProtobufUtils.createMapStructType(key);
                }
              });

  private static final ImmutableMap<LegacySQLTypeName, DescriptorProtos.FieldDescriptorProto.Type>
      BigQueryToProtoType =
          new ImmutableMap.Builder<LegacySQLTypeName, DescriptorProtos.FieldDescriptorProto.Type>()
              .put(LegacySQLTypeName.BYTES, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
              .put(LegacySQLTypeName.INTEGER, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(LegacySQLTypeName.BOOLEAN, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
              .put(LegacySQLTypeName.FLOAT, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
              .put(
                  LegacySQLTypeName.NUMERIC, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  LegacySQLTypeName.BIGNUMERIC,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(LegacySQLTypeName.STRING, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(LegacySQLTypeName.JSON, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  LegacySQLTypeName.TIMESTAMP,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(LegacySQLTypeName.DATE, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
              .put(
                  LegacySQLTypeName.DATETIME, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(
                  LegacySQLTypeName.GEOGRAPHY,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
              .build();
  private static final ImmutableMap<String, DescriptorProtos.FieldDescriptorProto.Type>
      SparkToProtoType =
          new ImmutableMap.Builder<String, DescriptorProtos.FieldDescriptorProto.Type>()
              .put(
                  DataTypes.BinaryType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
              .put(DataTypes.ByteType.json(), DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(
                  DataTypes.ShortType.json(), DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(
                  DataTypes.IntegerType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(DataTypes.LongType.json(), DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(
                  DataTypes.BooleanType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
              .put(
                  DataTypes.FloatType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
              .put(
                  DataTypes.DoubleType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
              .put(
                  NUMERIC_SPARK_TYPE.json(), DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  BigQueryDataTypes.BigNumericType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  DataTypes.StringType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  DataTypes.TimestampType.json(),
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(DataTypes.DateType.json(), DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
              .build();
  private static final ImmutableMap<Field.Mode, DescriptorProtos.FieldDescriptorProto.Label>
      BigQueryModeToProtoFieldLabel =
          new ImmutableMap.Builder<Field.Mode, DescriptorProtos.FieldDescriptorProto.Label>()
              .put(Field.Mode.NULLABLE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
              .put(Field.Mode.REPEATED, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
              .put(Field.Mode.REQUIRED, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
              .build();

  /** BigQuery Schema ==> ProtoSchema converter utils: */
  public static ProtoSchema toProtoSchema(Schema schema) throws IllegalArgumentException {
    try {
      Descriptors.Descriptor descriptor = toDescriptor(schema);
      return ProtoSchemaConverter.convert(descriptor);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException("Could not build Proto-Schema from BigQuery schema", e);
    }
  }

  public static ProtoSchema toProtoSchema(StructType schema) throws IllegalArgumentException {
    try {
      Descriptors.Descriptor descriptor = toDescriptor(schema);
      return ProtoSchemaConverter.convert(descriptor);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException("Could not build Proto-Schema from Spark schema", e);
    }
  }

  private static Descriptors.Descriptor toDescriptor(Schema schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");

    FieldList fields = schema.getFields();

    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, fields, initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  private static Descriptors.Descriptor createDescriptorFromProto(
      DescriptorProtos.DescriptorProto descriptorProto)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.FileDescriptorProto fileDescriptorProto =
        DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();

    Descriptors.Descriptor descriptor =
        Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProto, new Descriptors.FileDescriptor[] {})
            .getMessageTypes()
            .get(0);

    return descriptor;
  }

  @VisibleForTesting
  protected static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
      DescriptorProtos.DescriptorProto.Builder descriptorBuilder, FieldList fields, int depth) {
    Preconditions.checkArgument(
        depth < MAX_BIGQUERY_NESTED_DEPTH,
        "Tried to convert a BigQuery schema that exceeded BigQuery maximum nesting depth");
    int messageNumber = 1;
    for (Field field : fields) {
      String fieldName = field.getName();
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel = toProtoFieldLabel(field.getMode());
      FieldList subFields = field.getSubFields();

      if (field.getType() == LegacySQLTypeName.RECORD) {
        String recordTypeName =
            RESERVED_NESTED_TYPE_NAME
                + messageNumber; // TODO: Maintain this as a reserved nested-type name, which no
        // column can have.
        DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder =
            descriptorBuilder.addNestedTypeBuilder();
        nestedFieldTypeBuilder.setName(recordTypeName);
        DescriptorProtos.DescriptorProto nestedFieldType =
            buildDescriptorProtoWithFields(nestedFieldTypeBuilder, subFields, depth + 1);

        descriptorBuilder.addField(
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber)
                .setTypeName(recordTypeName));
      } else {
        DescriptorProtos.FieldDescriptorProto.Type fieldType = toProtoFieldType(field.getType());
        descriptorBuilder.addField(
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType));
      }
      messageNumber++;
    }
    return descriptorBuilder.build();
  }

  private static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName, DescriptorProtos.FieldDescriptorProto.Label fieldLabel, int messageNumber) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName(fieldName)
        .setLabel(fieldLabel)
        .setNumber(messageNumber);
  }

  @VisibleForTesting
  protected static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName,
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel,
      int messageNumber,
      DescriptorProtos.FieldDescriptorProto.Type fieldType) {
    return createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setType(fieldType);
  }

  private static DescriptorProtos.FieldDescriptorProto.Label toProtoFieldLabel(Field.Mode mode) {
    return Preconditions.checkNotNull(
        BigQueryModeToProtoFieldLabel.get(mode),
        new IllegalArgumentException("A BigQuery Field Mode was invalid: " + mode.name()));
  }

  // NOTE: annotations for DATETIME and TIMESTAMP objects are currently unsupported for external
  // users,
  // but if they become available, it would be advisable to append an annotation to the
  // protoFieldBuilder
  // for these and other types.
  private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(
      LegacySQLTypeName bqType) {
    if (LegacySQLTypeName.RECORD.equals(bqType)) {
      throw new IllegalStateException(
          "Program attempted to return an atomic data-type for a RECORD");
    }
    return Preconditions.checkNotNull(
        BigQueryToProtoType.get(bqType),
        new IllegalArgumentException("Unexpected type: " + bqType.name()));
  }

  /**
   * Spark Row --> ProtoRows converter utils: To be used by the DataWriters facing the BigQuery
   * Storage Write API
   */
  public static ProtoRows toProtoRows(StructType sparkSchema, InternalRow[] rows) {
    try {
      Descriptors.Descriptor schemaDescriptor = toDescriptor(sparkSchema);
      ProtoRows.Builder protoRows = ProtoRows.newBuilder();
      for (InternalRow row : rows) {
        DynamicMessage rowMessage = buildSingleRowMessage(sparkSchema, schemaDescriptor, row);
        protoRows.addSerializedRows(rowMessage.toByteString());
      }
      return protoRows.build();
    } catch (Exception e) {
      throw new RuntimeException("Could not convert Internal Rows to Proto Rows.", e);
    }
  }

  public static DynamicMessage buildSingleRowMessage(
      StructType schema, Descriptors.Descriptor schemaDescriptor, InternalRow row) {
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(schemaDescriptor);

    for (int fieldIndex = 0; fieldIndex < schemaDescriptor.getFields().size(); fieldIndex++) {
      int protoFieldNumber = fieldIndex + 1;

      StructField sparkField = schema.fields()[fieldIndex];
      DataType sparkType = sparkField.dataType();

      Object sparkValue = row.get(fieldIndex, sparkType);
      boolean nullable = sparkField.nullable();
      Descriptors.Descriptor nestedTypeDescriptor =
          schemaDescriptor.findNestedTypeByName(RESERVED_NESTED_TYPE_NAME + (protoFieldNumber));
      Object protoValue =
          convertSparkValueToProtoRowValue(sparkType, sparkValue, nullable, nestedTypeDescriptor);

      if (protoValue == null) {
        continue;
      }

      messageBuilder.setField(schemaDescriptor.findFieldByNumber(protoFieldNumber), protoValue);
    }

    return messageBuilder.build();
  }

  public static Descriptors.Descriptor toDescriptor(StructType schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");

    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, schema.fields(), initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  /*
  Takes a value in Spark format and converts it into ProtoRows format (to eventually be given to BigQuery).
   */
  private static Object convertSparkValueToProtoRowValue(
      DataType sparkType,
      Object sparkValue,
      boolean nullable,
      Descriptors.Descriptor nestedTypeDescriptor) {
    if (sparkValue == null) {
      if (!nullable) {
        throw new IllegalArgumentException("Non-nullable field was null.");
      } else {
        return null;
      }
    }

    if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      DataType elementType = arrayType.elementType();
      boolean containsNull = arrayType.containsNull();
      List<Object> protoValue = new ArrayList<>();
      // having issues to convert WrappedArray to Object[] in Java
      if (sparkValue instanceof ArrayData) {
        Object[] sparkArrayData = ((ArrayData) sparkValue).toObjectArray(elementType);
        for (Object sparkElement : sparkArrayData) {
          Object converted =
              convertSparkValueToProtoRowValue(
                  elementType, sparkElement, containsNull, nestedTypeDescriptor);
          if (converted == null) {
            continue;
          }
          protoValue.add(converted);
        }
      } else {
        IndexedSeq<Object> sparkArrayData = (IndexedSeq<Object>) sparkValue;
        int sparkArrayDataLength = sparkArrayData.length();
        for (int i = 0; i < sparkArrayDataLength; i++) {
          Object converted =
              convertSparkValueToProtoRowValue(
                  elementType, sparkArrayData.apply(i), containsNull, nestedTypeDescriptor);
          if (converted == null) {
            continue;
          }
          protoValue.add(converted);
        }
      }
      return protoValue;
    }

    if (sparkType instanceof StructType) {
      InternalRow internalRow = null;
      if (sparkValue instanceof Row) {
        internalRow = SparkSqlUtils.getInstance().rowToInternalRow((Row) sparkValue);
      } else {
        internalRow = (InternalRow) sparkValue;
      }
      return buildSingleRowMessage((StructType) sparkType, nestedTypeDescriptor, internalRow);
    }

    if (sparkType instanceof ByteType
        || sparkType instanceof ShortType
        || sparkType instanceof IntegerType
        || sparkType instanceof LongType) {
      return ((Number) sparkValue).longValue();
    }
    if (sparkType instanceof TimestampType) {
      return SparkBigQueryUtil.sparkTimestampToBigQuery(sparkValue);
    } // TODO: CalendarInterval

    if (sparkType instanceof DateType) {
      return SparkBigQueryUtil.sparkDateToBigQuery(sparkValue);
    }

    if (sparkType instanceof FloatType || sparkType instanceof DoubleType) {
      return ((Number) sparkValue).doubleValue();
    }

    if (sparkType instanceof DecimalType) {
      return convertDecimalToString(sparkValue);
    }

    if (sparkType instanceof BigNumericUDT) {
      return sparkValue.toString();
    }

    if (sparkType instanceof BooleanType) {
      return sparkValue;
    }

    if (sparkType instanceof BinaryType) {
      return sparkValue;
    }

    if (sparkType instanceof StringType) {
      return sparkValue.toString();
    }

    if (sparkType instanceof MapType) {
      // converting the map into ARRAY<STRUCT<key, value>> and making a recursive call
      MapType mapType = (MapType) sparkType;
      // As the same map<key, value> will be converted to the same STRUCT<key, value>, it is best to
      // cache this conversion instead of re-creating it for every value
      StructType mapStructType = MAP_TYPE_STRUCT_TYPE_CACHE.getUnchecked(mapType);
      List<InternalRow> entries = new ArrayList<>();
      if (sparkValue instanceof scala.collection.Map) {
        // Spark 3.x
        scala.collection.Map map = (scala.collection.Map) sparkValue;
        map.foreach(
            new Function1<Tuple2, Object>() {
              @Override
              public Object apply(Tuple2 entry) {
                return entries.add(new GenericInternalRow(new Object[] {entry._1(), entry._2()}));
              }

              @Override
              public <A> Function1<A, Object> compose(Function1<A, Tuple2> g) {
                return Function1.super.compose(g);
              }

              @Override
              public <A> Function1<Tuple2, A> andThen(Function1<Object, A> g) {
                return Function1.super.andThen(g);
              }
            });
      } else {
        // Spark 2.4
        MapData map = (MapData) sparkValue;
        Object[] keys = map.keyArray().toObjectArray(mapType.keyType());
        Object[] values = map.valueArray().toObjectArray(mapType.valueType());
        for (int i = 0; i < map.numElements(); i++) {
          Object key =
              convertSparkValueToProtoRowValue(
                  mapType.keyType(), keys[i], /* nullable */ false, nestedTypeDescriptor);
          Object value =
              convertSparkValueToProtoRowValue(
                  mapType.valueType(),
                  values[i],
                  mapType.valueContainsNull(),
                  nestedTypeDescriptor);
          entries.add(new GenericInternalRow(new Object[] {key, value}));
        }
      }
      ArrayData resultArray = ArrayData.toArrayData(entries.stream().toArray());
      ArrayType resultArrayType = ArrayType.apply(mapStructType, /* containsNull */ false);
      return convertSparkValueToProtoRowValue(
          resultArrayType, resultArray, nullable, nestedTypeDescriptor);
    }

    throw new IllegalStateException("Unexpected type: " + sparkType);
  }

  private static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
      DescriptorProtos.DescriptorProto.Builder descriptorBuilder, StructField[] fields, int depth) {
    Preconditions.checkArgument(
        depth < MAX_BIGQUERY_NESTED_DEPTH, "Spark Schema exceeds BigQuery maximum nesting depth.");
    int messageNumber = 1;
    for (StructField field : fields) {
      String fieldName = field.name();
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel =
          field.nullable()
              ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL
              : DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;

      DataType sparkType = field.dataType();

      if (sparkType instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) sparkType;
        /* DescriptorProtos.FieldDescriptorProto.Label elementLabel = arrayType.containsNull() ?
        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL :
        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED; TODO: how to support null instances inside an array (repeated field) in BigQuery?*/
        sparkType = arrayType.elementType();
        fieldLabel = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
      }

      DescriptorProtos.FieldDescriptorProto.Builder protoFieldBuilder;
      if (sparkType instanceof StructType || sparkType instanceof MapType) {
        String nestedName =
            RESERVED_NESTED_TYPE_NAME
                + messageNumber; // TODO: Maintain this as a reserved nested-type name, which no
        // column can have.
        StructField[] subFields = null;
        if (sparkType instanceof StructType) {
          StructType structType = (StructType) sparkType;
          subFields = structType.fields();
        } else {
          // Map type
          MapType mapType = (MapType) sparkType;
          fieldLabel = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
          subFields = createMapStructFields(mapType);
        }

        DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder =
            descriptorBuilder.addNestedTypeBuilder().setName(nestedName);
        buildDescriptorProtoWithFields(nestedFieldTypeBuilder, subFields, depth + 1);

        protoFieldBuilder =
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setTypeName(nestedName);
      } else {
        DescriptorProtos.FieldDescriptorProto.Type fieldType = toProtoFieldType(sparkType);
        protoFieldBuilder =
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType);
      }
      descriptorBuilder.addField(protoFieldBuilder);
      messageNumber++;
    }
    return descriptorBuilder.build();
  }

  static StructType createMapStructType(MapType mapType) {
    return new StructType(createMapStructFields(mapType));
  }

  @NotNull
  static StructField[] createMapStructFields(MapType mapType) {
    return new StructField[] {
      StructField.apply( //
          "key", //
          mapType.keyType(), //
          /* nullable */ false, //
          Metadata.empty()),
      StructField.apply( //
          "value", //
          mapType.valueType(), //
          mapType.valueContainsNull(), //
          Metadata.empty())
    };
  }

  private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(DataType sparkType) {
    if (sparkType instanceof MapType) {;
    }
    return Preconditions.checkNotNull(
        SparkToProtoType.get(sparkType.json()),
        new IllegalStateException("Unexpected type: " + sparkType));
  }

  private static String convertDecimalToString(Object decimal) {
    BigDecimal bigDecimal =
        decimal instanceof Decimal ? ((Decimal) decimal).toJavaBigDecimal() : (BigDecimal) decimal;
    return bigDecimal.toPlainString();
  }
}
