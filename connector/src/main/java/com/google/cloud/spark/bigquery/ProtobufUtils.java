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

import avro.shaded.com.google.common.base.Preconditions;
import com.google.cloud.ByteArray;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ProtobufUtils {

    // The maximum nesting depth of a BigQuery RECORD:
    private static final int MAX_BIGQUERY_NESTED_DEPTH = 15;
    // For every message, a nested type is name "STRUCT"+i, where i is the
    // number of the corresponding field that is of this type in the containing message.
    private static final String RESERVED_NESTED_TYPE_NAME = "STRUCT";
    private static final String MAPTYPE_ERROR_MESSAGE = "MapType is unsupported.";

    /**
     * BigQuery Schema ==> ProtoSchema converter utils:
     */
    public static ProtoBufProto.ProtoSchema toProtoSchema (Schema schema) throws Exception {
        try{
            Descriptors.Descriptor descriptor = toDescriptor(schema);
            ProtoBufProto.ProtoSchema protoSchema = ProtoSchemaConverter.convert(descriptor);
            return protoSchema;
        } catch (Descriptors.DescriptorValidationException e){
            throw new Exception("Could not build Proto-Schema from Spark schema.", e); // TODO: right exception to throw?
        }
    }

    private static Descriptors.Descriptor toDescriptor (Schema schema) throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto.Builder descriptorBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Schema");

        FieldList fields = schema.getFields();

        DescriptorProtos.DescriptorProto descriptorProto = buildDescriptorProtoWithFields(descriptorBuilder, fields, 0);

        return createDescriptorFromProto(descriptorProto);
    }

    private static Descriptors.Descriptor createDescriptorFromProto(DescriptorProtos.DescriptorProto descriptorProto)
            throws Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto
                .newBuilder()
                .addMessageType(descriptorProto)
                .build();

        Descriptors.Descriptor descriptor = Descriptors.FileDescriptor
                .buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[]{})
                .getMessageTypes()
                .get(0);

        return descriptor;
    }

    @VisibleForTesting
    protected static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
            DescriptorProtos.DescriptorProto.Builder descriptorBuilder, FieldList fields, int depth){
        Preconditions.checkArgument(depth < MAX_BIGQUERY_NESTED_DEPTH,
                "Tried to convert a BigQuery schema that exceeded BigQuery maximum nesting depth");
        int messageNumber = 1;
        for (Field field : fields) {
            String fieldName = field.getName();
            DescriptorProtos.FieldDescriptorProto.Label fieldLabel = toProtoFieldLabel(field.getMode());
            FieldList subFields = field.getSubFields();

            if (field.getType() == LegacySQLTypeName.RECORD){
                String recordTypeName = "RECORD"+messageNumber;  // TODO: Change or assert this to be a reserved name. No column can have this name.
                DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder = descriptorBuilder.addNestedTypeBuilder();
                nestedFieldTypeBuilder.setName(recordTypeName);
                DescriptorProtos.DescriptorProto nestedFieldType = buildDescriptorProtoWithFields(
                        nestedFieldTypeBuilder, subFields, depth+1);

                descriptorBuilder.addField(createProtoFieldBuilder(fieldName, fieldLabel, messageNumber)
                        .setTypeName(recordTypeName));
            }
            else {
                DescriptorProtos.FieldDescriptorProto.Type fieldType = toProtoFieldType(field.getType());
                descriptorBuilder.addField(createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType));
            }
            messageNumber++;
        }
        return descriptorBuilder.build();
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
            String fieldName, DescriptorProtos.FieldDescriptorProto.Label fieldLabel, int messageNumber) {
        return DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName(fieldName)
                .setLabel(fieldLabel)
                .setNumber(messageNumber);
    }

    @VisibleForTesting
    protected static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
            String fieldName, DescriptorProtos.FieldDescriptorProto.Label fieldLabel, int messageNumber,
            DescriptorProtos.FieldDescriptorProto.Type fieldType) {
        return DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName(fieldName)
                .setLabel(fieldLabel)
                .setNumber(messageNumber)
                .setType(fieldType);
    }

    private static DescriptorProtos.FieldDescriptorProto.Label toProtoFieldLabel(Field.Mode mode) {
        switch (mode) {
            case NULLABLE:
                return DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
            case REPEATED:
                return DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
            case REQUIRED:
                return DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
            default:
                throw new IllegalArgumentException("A BigQuery Field Mode was invalid: "+mode.name());
        }
    }

    // NOTE: annotations for DATETIME and TIMESTAMP objects are currently unsupported for external users,
    // but if they become available, it would be advisable to append an annotation to the protoFieldBuilder
    // for these and other types.
    private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(LegacySQLTypeName bqType) {
        DescriptorProtos.FieldDescriptorProto.Type protoFieldType;
        if (LegacySQLTypeName.INTEGER.equals(bqType) ||
                LegacySQLTypeName.DATE.equals(bqType) ||
                LegacySQLTypeName.DATETIME.equals(bqType) ||
                LegacySQLTypeName.TIMESTAMP.equals(bqType)) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
        }
        if (LegacySQLTypeName.BOOLEAN.equals(bqType)){
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
        }
        if (LegacySQLTypeName.STRING.equals(bqType)) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
        }
        if (LegacySQLTypeName.GEOGRAPHY.equals(bqType) ||
                LegacySQLTypeName.BYTES.equals(bqType) ||
                LegacySQLTypeName.NUMERIC.equals(bqType)) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
        }
        if (LegacySQLTypeName.FLOAT.equals(bqType)) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
        }
        else {
            if (LegacySQLTypeName.RECORD.equals(bqType)) {
                throw new IllegalStateException("Program attempted to return an atomic data-type for a RECORD");
            }
            throw new IllegalArgumentException("Unexpected type: " + bqType.name());
        }
    }


    /**
     * Spark Row --> ProtoRows converter utils:
     * To be used by the DataWriters facing the BigQuery Storage Write API
     */
    public static ProtoBufProto.ProtoRows toProtoRows(StructType sparkSchema, InternalRow[] rows) {
        try {
            Descriptors.Descriptor schemaDescriptor = toDescriptor(sparkSchema);
            ProtoBufProto.ProtoRows.Builder protoRows = ProtoBufProto.ProtoRows.newBuilder();
            for (InternalRow row : rows) {
                DynamicMessage rowMessage = createSingleRowMessage(sparkSchema,
                        schemaDescriptor, row);
                protoRows.addSerializedRows(rowMessage.toByteString());
            }
            return protoRows.build();
        } catch (Exception e) {
            throw new RuntimeException("Could not convert Internal Rows to Proto Rows.", e);
        }
    }

    public static DynamicMessage createSingleRowMessage(StructType schema,
                                                           Descriptors.Descriptor schemaDescriptor,
                                                           InternalRow row) {

        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(schemaDescriptor);

        for(int i = 1; i <= schemaDescriptor.getFields().size(); i++) {
            StructField sparkField = schema.fields()[i-1];
            DataType sparkType = sparkField.dataType();
            if (sparkType instanceof StructType) {
                messageBuilder.setField(schemaDescriptor.findFieldByNumber(i),
                        createSingleRowMessage((StructType)sparkType,
                                schemaDescriptor.findNestedTypeByName(RESERVED_NESTED_TYPE_NAME +i),
                                (InternalRow)row.get(i-1, sparkType)));
            }
            else {
                Object converted = convert(sparkField, row.get(i-1, sparkType));
                if (converted == null) {
                    continue;
                }
                messageBuilder.setField(schemaDescriptor.findFieldByNumber(i),
                        convert(sparkField,
                                row.get(i-1, sparkType)));
            }
        }

        return messageBuilder.build();
    }

    public static Descriptors.Descriptor toDescriptor (StructType schema)
            throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto.Builder descriptorBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Schema");

        StructField[] fields = schema.fields();

        DescriptorProtos.DescriptorProto descriptorProto = buildDescriptorProtoWithFields(descriptorBuilder, fields, 0);

        return createDescriptorFromProto(descriptorProto);
    }

    @VisibleForTesting
    protected static Object convert (StructField sparkField, Object sparkValue) {
        if (sparkValue == null) {
            if (!sparkField.nullable()) {
                throw new IllegalArgumentException("Non-nullable field was null.");
            }
            else {
                return null;
            }
        }

        DataType fieldType = sparkField.dataType();

        if (fieldType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType)fieldType;
            boolean containsNull = arrayType.containsNull(); // elements can be null.
            DataType elementType = arrayType.elementType();

            ArrayData arrayData = (ArrayData)sparkValue;
            Object[] sparkValues = arrayData.toObjectArray(elementType);

            return Arrays.stream(sparkValues).map(value -> {
                Preconditions.checkArgument(containsNull || value != null,
                        "Encountered a null value inside a non-null-containing array.");
                return toAtomicProtoRowValue(elementType, value);
            } ).collect(Collectors.toList());
        }
        else {
            return toAtomicProtoRowValue(fieldType, sparkValue);
        }
    }

    /*
    Takes a value in Spark format and converts it into ProtoRows format (to eventually be given to BigQuery).
     */
    private static Object toAtomicProtoRowValue(DataType sparkType, Object value) {
        if (sparkType instanceof ByteType ||
                sparkType instanceof ShortType ||
                sparkType instanceof IntegerType ||
                sparkType instanceof LongType) {
            return ((Number)value).longValue();
        }

        if (sparkType instanceof FloatType ||
                sparkType instanceof DoubleType ||
                sparkType instanceof DecimalType) {
            return ((Number)value).doubleValue(); // TODO: should decimal be converted to double? Or a Bytes type containing extra width?
        }

        if (sparkType instanceof TimestampType) {
            return Timestamp.valueOf((String)value).getTime(); //
        }

        if (sparkType instanceof DateType) {
            return Date.valueOf((String)value).getTime();
        } // TODO: CalendarInterval

        if (sparkType instanceof BooleanType) {
            return value;
        }

        if (sparkType instanceof StringType) {
            return new String(((UTF8String)value).getBytes());
        }

        if (sparkType instanceof BinaryType) {
            return ((ByteArray)value).toByteArray();
        }

        if (sparkType instanceof MapType) {
            throw new IllegalArgumentException(MAPTYPE_ERROR_MESSAGE);
        }

        throw new IllegalStateException("Unexpected type: " + sparkType);
    }

    private static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
            DescriptorProtos.DescriptorProto.Builder descriptorBuilder, StructField[] fields, int depth) {
        Preconditions.checkArgument(depth < MAX_BIGQUERY_NESTED_DEPTH,
                "Spark Schema exceeds BigQuery maximum nesting depth.");
        int messageNumber = 1;
        for (StructField field : fields) {
            String fieldName = field.name();
            DescriptorProtos.FieldDescriptorProto.Label fieldLabel =  field.nullable() ?
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL :
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
            DescriptorProtos.FieldDescriptorProto.Type fieldType;

            DataType sparkType = field.dataType();
            if (sparkType instanceof StructType) {
                StructType structType = (StructType)sparkType;
                String nestedName = RESERVED_NESTED_TYPE_NAME +messageNumber; // TODO: this should be a reserved name. No column can have this name.
                StructField[] subFields = structType.fields();

                DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder = descriptorBuilder.addNestedTypeBuilder()
                        .setName(nestedName);
                buildDescriptorProtoWithFields(nestedFieldTypeBuilder, subFields, depth+1);

                descriptorBuilder.addField(createProtoFieldBuilder(fieldName, fieldLabel, messageNumber)
                        .setTypeName(nestedName));
                messageNumber++;
                continue;
            }

            if (sparkType instanceof ArrayType) {
                ArrayType arrayType = (ArrayType)sparkType;
                /* DescriptorProtos.FieldDescriptorProto.Label elementLabel = arrayType.containsNull() ?
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL :
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED; TODO: how to support null instances inside an array (repeated field) in BigQuery?*/
                fieldType = sparkAtomicTypeToProtoFieldType(arrayType.elementType());
                fieldLabel = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;

            } else {
                fieldType = sparkAtomicTypeToProtoFieldType(sparkType);
            }
            descriptorBuilder.addField(
                    createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType));
            messageNumber++;
        }
        return descriptorBuilder.build();
    }

    // NOTE: annotations for DATETIME and TIMESTAMP objects are currently unsupported for external users,
    // but if they become available, it would be advisable to append an annotation to the protoFieldBuilder
    // for these and other types.
    // This function only converts atomic Spark DataTypes
    private static DescriptorProtos.FieldDescriptorProto.Type sparkAtomicTypeToProtoFieldType(DataType sparkType) {
        if (sparkType instanceof ByteType ||
                sparkType instanceof ShortType ||
                sparkType instanceof IntegerType ||
                sparkType instanceof LongType ||
                sparkType instanceof TimestampType ||
                sparkType instanceof DateType) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
        }

        if (sparkType instanceof FloatType ||
                sparkType instanceof DoubleType ||
                sparkType instanceof DecimalType) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            // TODO: an annotation to distinguish between decimals that are doubles, and decimals that are NUMERIC (Bytes types)
        }

        if (sparkType instanceof BooleanType) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
        }

        if (sparkType instanceof BinaryType) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
        }

        if (sparkType instanceof StringType) {
            return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
        }

        if (sparkType instanceof MapType) {
            throw new IllegalArgumentException(MAPTYPE_ERROR_MESSAGE);
        }

        throw new IllegalStateException("Unexpected type: " + sparkType);
    }
}
