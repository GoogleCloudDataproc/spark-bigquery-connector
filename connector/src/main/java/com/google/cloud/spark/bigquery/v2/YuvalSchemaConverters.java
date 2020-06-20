package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoSchemaConverter;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class YuvalSchemaConverters {

    // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
    private final static int BQ_NUMERIC_PRECISION = 38;
    private final static int BQ_NUMERIC_SCALE = 9;
    private final static DecimalType NUMERIC_SPARK_TYPE = DataTypes.createDecimalType(
            BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);
    // The maximum nested depth of a RECORD in a BigQuery table is 15:
    private static final int MAX_BQ_NESTED_DEPTH = 15;

    private static final Logger logger = LoggerFactory.getLogger(YuvalSchemaConverters.class);


    /*
    SECTION 1
    Spark ==> BigQuery Schema Converter:
     */
    public static Table createTable(BigQuery bigquery, TableId tableId, StructType sparkSchema){
        Schema bqSchema = toBQSchema(sparkSchema);
        TableDefinition tableDefinition = StandardTableDefinition.of(bqSchema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        return bigquery.create(tableInfo);
    }

    /*
    Create a BigQuery Schema given a Spark schema.
     */
    public static Schema toBQSchema(StructType sparkSchema) {
        FieldList bqFields = sparkToBQFields(sparkSchema);
        return Schema.of(bqFields);
    }

    /*
    Returns a FieldList of all the Spark StructField objects converted to BigQuery Field objects
     */
    private static FieldList sparkToBQFields(StructType sparkStruct){
        List<Field> bqFields = new ArrayList<>();
        StructField[] sparkFields = sparkStruct.fields();
        String[] sparkFieldNames = sparkStruct.fieldNames();
        for(String fieldName : sparkFieldNames){
            bqFields.add(makeBQColumn(fieldName, sparkFields[(int)sparkStruct.getFieldIndex(fieldName).get()]));
        }
        return FieldList.of(bqFields);
    }

    /*
    Given a StructField and its name, returns the corresponding BigQuery Field
     */
    private static Field makeBQColumn(String fieldName, StructField sparkField) {
        DataType sparkType = sparkField.dataType();
        String columnName = sparkField.name();
        Field.Mode mode = (sparkField.nullable()) ? Field.Mode.NULLABLE : Field.Mode.REQUIRED;

        if(sparkType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType)sparkType;

            String elementName = ""; // TODO: what is the name of a field in an array? Empty?
            LegacySQLTypeName elementType = toBQType(arrayType.elementType());
            Field elementField = makeField(elementName, elementType, Field.Mode.REPEATED, null);

            return makeField(columnName, LegacySQLTypeName.RECORD, mode, FieldList.of(elementField));
        }
        else if (sparkType instanceof MapType) {
            MapType mapType = (MapType)sparkType;

            String keyName = "K"; // TODO: what is the name of a key for a map in BQ?
            LegacySQLTypeName keyType = toBQType(mapType.keyType());
            Field keyField = makeField(keyName, keyType, mode, null); // TODO: what is the mode of a key in a map?

            String valueName = "V"; // TODO: what is the name of a value field for a map in BQ?
            LegacySQLTypeName valueType = toBQType(mapType.valueType());
            Field valueField = makeField(valueName, valueType, mode, null); // TODO: what is the mode of a value in a map?

            String pairName = "Pair"; // TODO: how to name the K,V pair?
            Field pairField = makeField(pairName, LegacySQLTypeName.RECORD, Field.Mode.REPEATED,
                    FieldList.of(keyField, valueField));

            return makeField(columnName, LegacySQLTypeName.RECORD, mode, FieldList.of(pairField));
        }
        else if (sparkType instanceof StructType) {
            FieldList variableFields = sparkToBQFields((StructType)sparkType);

            return makeField(columnName, LegacySQLTypeName.RECORD, mode, variableFields);
        }
        else {
            LegacySQLTypeName columnType = toBQType(sparkType);
            return makeField(columnName, columnType, mode, null);
        }
    }

    /*
    Returns the BigQuery Data-Type corresponding to a Spark DataType.
     */
    private static LegacySQLTypeName toBQType(DataType elementType) {
        if (elementType instanceof BinaryType) {
            return LegacySQLTypeName.BYTES;
        } else if (elementType instanceof ByteType ||
                elementType instanceof ShortType ||
                elementType instanceof IntegerType ||
                elementType instanceof LongType) {
            return LegacySQLTypeName.INTEGER;
        } else if (elementType instanceof BooleanType) {
            return LegacySQLTypeName.BOOLEAN;
        } else if (elementType instanceof FloatType ||
                elementType instanceof DoubleType) {
            return LegacySQLTypeName.FLOAT;
        } else if (elementType instanceof DecimalType) { // TODO
            DecimalType decimalType = (DecimalType)elementType;
            if (decimalType.isTighterThan(DataTypes.DoubleType)) {
                return LegacySQLTypeName.FLOAT;
            } else if (decimalType.precision() <= BQ_NUMERIC_PRECISION &&
                    decimalType.scale() <= BQ_NUMERIC_SCALE) {
                return LegacySQLTypeName.NUMERIC;
            } else {
                throw new IllegalStateException("Decimal type is too wide to fit in BigQuery Numeric format");
            }
        } else if (elementType instanceof StringType) {
            return LegacySQLTypeName.STRING;
        } else if (elementType instanceof TimestampType) {
            return LegacySQLTypeName.TIMESTAMP;
        } else if (elementType instanceof DateType) {
            return LegacySQLTypeName.DATE;
        } else {
            throw new IllegalStateException("Data type not expected in toBQType: "+elementType.simpleString());
        }
    }

    /*
    Helper function to simply make a field, after all parameters (name, type, mode, and sub-fields) have been extracted.
     */
    private static Field makeField(String name, LegacySQLTypeName type, Field.Mode mode, FieldList subfields){
        Field field = Field.newBuilder(name, type, subfields)
                .setMode(mode)
                .build();
        logger.info("Created field: "+field.toString());
        return field;
    }



    /*
    SECTION 2
    BigQuery Schema ==> ProtoSchema
     */
    // create DescriptorProto
    // for every field, create FieldDescriptorProto
    // DescriptorProto.addField(FieldDescriptorProto)-
    // buildFrom()
    // FileDescriptor.getMessageTypes[0]
    public static ProtoBufProto.ProtoSchema toProtoSchema(Schema schema){
        try{
            Descriptors.Descriptor descriptor = toDescriptor(schema);
            return ProtoSchemaConverter.convert(descriptor);
        } catch (Descriptors.DescriptorValidationException e){
            logger.error("Descriptor Validation Exception");
            e.printStackTrace();
            return null;
        }
    }

    /*
    Creates a descriptor for a given BigQuery Schema
     */
    public static Descriptors.Descriptor toDescriptor(Schema schema) throws Descriptors.DescriptorValidationException {
        DescriptorProtos.DescriptorProto.Builder descriptorBuilder = DescriptorProtos.DescriptorProto.newBuilder().setName("SparkRow");

        FieldList fields = schema.getFields();

        DescriptorProtos.DescriptorProto descriptorProto = buildDescriptorWithFields(descriptorBuilder, fields, 0);

        DescriptorProtos.FileDescriptorProto fileDescriptorProto = DescriptorProtos.FileDescriptorProto
                .newBuilder()
                .addMessageType(descriptorProto)
                .build();

        Descriptors.Descriptor descriptor = Descriptors.FileDescriptor
                .buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[]{})
                .getMessageTypes()
                .get(0);

        logger.info("Created descriptor proto: "+descriptor.getFields());
        return descriptor;
    }

    /*
    Takes a ProtoDescriptor and a FieldList object, and builds the ProtoDescriptor according to the Schema FieldList.
    Supports nested types in the Schema, but up to a maximum depth (which is nominally 15 layers with BigQuery).
     */
    private static DescriptorProtos.DescriptorProto buildDescriptorWithFields(DescriptorProtos.DescriptorProto.Builder descriptorBuilder, FieldList fields, int depth){
        if(depth >= MAX_BQ_NESTED_DEPTH){
            return descriptorBuilder.build();
        }
        for (java.util.Iterator<Field> it = fields.iterator(); it.hasNext(); ) {
            Field field = it.next();
            String fieldName = field.getName();
            FieldList subFields = field.getSubFields();
            int index = fields.getIndex(fieldName); // TODO: can't be 0. This is probably the wrong index to use...

            if(subFields == null){
                descriptorBuilder.addField(makeProtoField(field, index));
            }
            else{
                DescriptorProtos.DescriptorProto.Builder parentFieldBuilder = DescriptorProtos.DescriptorProto.newBuilder();
                parentFieldBuilder.setName(fieldName);

                DescriptorProtos.DescriptorProto nestedField = buildDescriptorWithFields(parentFieldBuilder, subFields, depth+1);

                descriptorBuilder.addNestedType(index, nestedField);    // QUESTION: should this be a nested type or a repeated type? Appears to be automatically a repeated type
            }
        }
        return descriptorBuilder.build();
    }

    /*
    Given a BigQuery Schema Field object, and its index number in the Schema,
    Builds a FieldDescriptorProto object.
     */
    private static DescriptorProtos.FieldDescriptorProto makeProtoField(Field field, int number) {

        LegacySQLTypeName bqType = field.getType();
        String name = field.getName();
        String description = field.getDescription();    // FIXME: what to do with description?
        DescriptorProtos.FieldDescriptorProto.Label label = DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
        Field.Mode bqMode = field.getMode();

        if(bqMode == null) {
            // TODO: if Field mode is empty?? Possible?
        }
        else {
            switch (bqMode) {
                case NULLABLE:
                    label = DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
                case REPEATED:
                    label = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
                case REQUIRED:
                    label = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
            }
        }

        DescriptorProtos.FieldDescriptorProto protoField = setProtoFieldType(DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(label), bqType).build();

        logger.info("Created proto field descriptor: "+protoField);
        return protoField;
    }

    /*
    Given a BigQuery Schema Data-Type, returns the equivalent proto-buffer type.
     */
    // Extra annotations for DATETIME, etc.
    // Rethink this function: setter for annotation. Gets a FieldDescriptorBuilder and adds type.
    // Option B:
    private static DescriptorProtos.FieldDescriptorProto.Builder setProtoFieldType(
            DescriptorProtos.FieldDescriptorProto.Builder protoFieldBuilder, LegacySQLTypeName bqType) {
        DescriptorProtos.FieldDescriptorProto.Type protoFieldType;
        if (LegacySQLTypeName.INTEGER.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.DATE.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            // TODO: annotation
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.DATETIME.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            // TODO: annotation
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.TIMESTAMP.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            // TODO: annotation
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.BOOLEAN.equals(bqType)){
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.STRING.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.GEOGRAPHY.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            // TODO: annotation
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.BYTES.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.NUMERIC.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
            // TODO: annotation
            return protoFieldBuilder.setType(protoFieldType);
        }
        else if (LegacySQLTypeName.FLOAT.equals(bqType)) {
            protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
            return protoFieldBuilder.setType(protoFieldType);
        }
        else {
            if (LegacySQLTypeName.RECORD.equals(bqType)) {
                throw new IllegalStateException("Tried to create an atomic field's Data-Type, but received a RECORD / nested field");
            }
            throw new IllegalStateException("Unexpected type: " + bqType.name());
        }
    }

    /*
    SECTION 3
    Spark Row --> ProtoRows
     */
    // TODO
    public static ProtoBufProto.ProtoRows toBQRow(ProtoBufProto.ProtoSchema protoSchema, StructType schema, InternalRow row){
        ProtoBufProto.ProtoRows.Builder rowBuilder = ProtoBufProto.ProtoRows.newBuilder();
        StructField[] fields = schema.fields();
        for(int i = 0; i < fields.length; i++){
            // rowBuilder.
        }
        return null;
    }

    // TODO: change from sparkField to BigQuery Field...
    private static Object convert(StructField sparkField, Object value) {
        if (value == null) {
            if(sparkField.nullable()) return null;
            else throw new IllegalStateException("Required field received a null value");
        }

        DataType type = sparkField.dataType();

        if (type instanceof NumericType || type instanceof BooleanType) {
            return value;
        }

        // TODO: binary type

        if (type instanceof StringType ||
                type instanceof DateType ||
                type instanceof TimestampType) {
            return UTF8String.fromBytes(((Utf8) value).getBytes());
        }

        // TODO: non-atomic types

        throw new IllegalStateException("Unexpected type: " + type);
    }

}
