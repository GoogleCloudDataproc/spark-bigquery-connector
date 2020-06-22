package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.spark.bigquery.v2.YuvalSchemaConverters;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import static com.google.common.truth.Truth.*;

public class SchemaConverterTests {

    // TODO: logger here
    private final Logger logger = LogManager.getLogger("com.google.cloud.spark");

    // TODO: test for makeBQColumn

    @Test
    public void testToBQSchema1IntegerColumn() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = Spark1IntegerColumn;
        Schema check = BQ1IntegerColumn;

        Schema converted = YuvalSchemaConverters.toBigQuerySchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    @Test
    public void testToBQSchema2Columns() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = Spark2Columns;
        Schema check = BQ2Columns;

        Schema converted = YuvalSchemaConverters.toBigQuerySchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    @Test
    public void testToBQSchemaNestedStruct() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = SparkNestedStruct;
        Schema check = BQNestedStruct;

        Schema converted = YuvalSchemaConverters.toBigQuerySchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString()+"\n"+
                converted.getFields().get(0).getSubFields().toString());

        assertThat(converted).isEqualTo(check);
    }

    @Test
    public void testToBQSchemaArray() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = SparkArray;
        Schema check = BQArray;

        Schema converted = YuvalSchemaConverters.toBigQuerySchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    /*
    @Test
    public void testToBQSchemaMap() throws Exception {
        logger.setLevel(Level.DEBUG);

        StructType schema = SparkMap;
        Schema check = BQMap;

        Schema converted = YuvalSchemaConverters.toBigQuerySchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString()+"\n"+
                converted.getFields().get(0).getSubFields().toString()+"\n"+
                converted.getFields().get(0).getSubFields().get(0).getSubFields().toString());

        assertThat(converted).isEqualTo(check);
    }
     */

    @Test
    public void testToProtoSchema1IntegerColumn() throws Exception {
        logger.setLevel(Level.DEBUG);

        Schema schema = BQ1IntegerColumn;
        ProtoBufProto.ProtoSchema check = null; // TODO: make .proto file for this test

        ProtoBufProto.ProtoSchema converted = YuvalSchemaConverters.toProtoSchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    @Test
    public void testToProtoSchema2Columns() throws Exception {
        logger.setLevel(Level.DEBUG);

        Schema schema = BQ2Columns;
        ProtoBufProto.ProtoSchema check = null; // TODO: make .proto file for this test

        ProtoBufProto.ProtoSchema converted = YuvalSchemaConverters.toProtoSchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    @Test
    public void testToProtoSchemaNestedStruct() throws Exception {
        logger.setLevel(Level.DEBUG);

        Schema schema = BQNestedStruct;
        ProtoBufProto.ProtoSchema check = null; // TODO: make .proto file for this test

        ProtoBufProto.ProtoSchema converted = YuvalSchemaConverters.toProtoSchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    @Test
    public void testToProtoSchemaArray() throws Exception {
        logger.setLevel(Level.DEBUG);

        Schema schema = BQArray;
        ProtoBufProto.ProtoSchema check = null; // TODO: make .proto file for this test

        ProtoBufProto.ProtoSchema converted = YuvalSchemaConverters.toProtoSchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }

    /*
    @Test
    public void testToProtoSchemaMap() throws Exception {
        logger.setLevel(Level.DEBUG);

        Schema schema = BQMap;
        ProtoBufProto.ProtoSchema check = null; // TODO: make .proto file for this test

        ProtoBufProto.ProtoSchema converted = YuvalSchemaConverters.toProtoSchema(schema);

        LogManager.getLogger("com.google.cloud.spark").info(converted.toString());

        assertThat(converted).isEqualTo(check);
    }
     */

    public final StructType Spark1IntegerColumn = new StructType()
            .add(new StructField("Numbers", DataTypes.IntegerType, true, null));
    public final Schema BQ1IntegerColumn = Schema.of(Field.newBuilder("Numbers", LegacySQLTypeName.INTEGER,
            (FieldList)null).setMode(Field.Mode.NULLABLE).build());

    public final StructType Spark2Columns = new StructType().add(new StructField("Numbers", DataTypes.IntegerType,
            true, null))
            .add(new StructField("Strings", DataTypes.StringType,
                    false, null));
    public final Schema BQ2Columns = Schema.of(Field.newBuilder("Numbers", LegacySQLTypeName.INTEGER, (FieldList) null)
                    .setMode(Field.Mode.NULLABLE).build(),
            Field.newBuilder("Strings", LegacySQLTypeName.STRING, (FieldList) null)
                    .setMode(Field.Mode.REQUIRED).build());

    public final StructType SparkNestedStruct = new StructType().add(new StructField("Struct",
            DataTypes.createStructType(new StructField[]{new StructField("Number", DataTypes.IntegerType,
                    true, null),
                    new StructField("String", DataTypes.StringType,
                            true, null)}),
            true, null));
    public final Schema BQNestedStruct = Schema.of(Field.newBuilder("Struct", LegacySQLTypeName.RECORD,
            Field.newBuilder("Number", LegacySQLTypeName.INTEGER, (FieldList) null)
                    .setMode(Field.Mode.NULLABLE).build(),
            Field.newBuilder("String", LegacySQLTypeName.STRING, (FieldList) null)
                    .setMode(Field.Mode.NULLABLE).build())
            .setMode(Field.Mode.NULLABLE).build());

    public final StructType SparkArray = new StructType().add(new StructField("Array",
            DataTypes.createArrayType(DataTypes.IntegerType),
            true, null));
    public final Schema BQArray = Schema.of(Field.newBuilder("Array", LegacySQLTypeName.INTEGER, (FieldList) null)
            .setMode(Field.Mode.REPEATED).build());

    public final StructType SparkMap = new StructType().add(new StructField("Map",
            DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType),
            true, null));
    public final Schema BQMap = Schema.of(Field.newBuilder("Map", LegacySQLTypeName.RECORD,
            Field.newBuilder("Pair", LegacySQLTypeName.RECORD, FieldList.of(  // TODO: map - pair, k, v names?
                    Field.newBuilder("K", LegacySQLTypeName.INTEGER, (FieldList) null)
                            .setMode(Field.Mode.NULLABLE).build(),
                    Field.newBuilder("V", LegacySQLTypeName.STRING, (FieldList) null)
                            .setMode(Field.Mode.NULLABLE).build()
            )).setMode(Field.Mode.REPEATED).build())
            .setMode(Field.Mode.NULLABLE).build());
}