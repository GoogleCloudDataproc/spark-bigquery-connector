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
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.HivePartitioningOptions;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class BigQueryUtilTest {

  private static final TableId TABLE_ID =
      TableId.of("test.org:test-project", "test_dataset", "test_table");
  private static final String FULLY_QUALIFIED_TABLE =
      "test.org:test-project.test_dataset.test_table";

  @Test
  public void testParseFullyQualifiedTable() {
    TableId tableId = BigQueryUtil.parseTableId(FULLY_QUALIFIED_TABLE);
    assertThat(tableId).isEqualTo(TABLE_ID);
  }

  @Test
  public void testParseFullyQualifiedLegacyTable() {
    TableId tableId = BigQueryUtil.parseTableId("test.org:test-project.test_dataset.test_table");
    assertThat(tableId).isEqualTo(TABLE_ID);
  }

  @Test
  public void testParseInvalidTable() {
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryUtil.parseTableId("test-org:test-project.table"));
  }

  @Test
  public void testParseFullyQualifiedTableWithDefaults() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            FULLY_QUALIFIED_TABLE, Optional.of("other_dataset"), Optional.of("other-project"));
    assertThat(tableId).isEqualTo(TABLE_ID);
  }

  @Test
  public void testParsePartiallyQualifiedTable() {
    TableId tableId = BigQueryUtil.parseTableId("test_dataset.test_table");
    assertThat(tableId).isEqualTo(TableId.of("test_dataset", "test_table"));
  }

  @Test
  public void testParsePartiallyQualifiedTableWithDefaults() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_dataset.test_table",
            Optional.of("other_dataset"),
            Optional.of("default-project"));
    assertThat(tableId).isEqualTo(TableId.of("default-project", "test_dataset", "test_table"));
  }

  @Test
  public void testParseUnqualifiedTableWithDefaults() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_table", Optional.of("default_dataset"), Optional.of("default-project"));
    assertThat(tableId).isEqualTo(TableId.of("default-project", "default_dataset", "test_table"));
  }

  @Test
  public void testParseFullyQualifiedPartitionedTable() {
    TableId tableId = BigQueryUtil.parseTableId(FULLY_QUALIFIED_TABLE + "$12345");
    assertThat(tableId)
        .isEqualTo(TableId.of("test.org:test-project", "test_dataset", "test_table$12345"));
  }

  @Test
  public void testParseUnqualifiedPartitionedTable() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_table$12345", Optional.of("default_dataset"), Optional.empty());
    assertThat(tableId).isEqualTo(TableId.of("default_dataset", "test_table$12345"));
  }

  @Test
  public void testParseTableWithDatePartition() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_table",
            Optional.of("default_dataset"),
            Optional.empty(),
            Optional.of("20200101"));
    assertThat(tableId).isEqualTo(TableId.of("default_dataset", "test_table$20200101"));
  }

  @Test
  public void testUnparsableTable() {
    assertThrows(IllegalArgumentException.class, () -> BigQueryUtil.parseTableId("foo:bar:baz"));
  }

  @Test
  public void testFriendlyName() {
    String name = BigQueryUtil.friendlyTableName(TABLE_ID);
    assertThat(name).isEqualTo(FULLY_QUALIFIED_TABLE);
  }

  @Test
  public void testShortFriendlyName() {
    String name = BigQueryUtil.friendlyTableName(TableId.of("test_dataset", "test_table"));
    assertThat(name).isEqualTo("test_dataset.test_table");
  }

  @Test
  public void testConvertAndThrows() {
    final BigQueryError bigQueryError = new BigQueryError("reason", "location", "message");
    BigQueryException bigQueryException =
        assertThrows(BigQueryException.class, () -> BigQueryUtil.convertAndThrow(bigQueryError));
    assertThat(bigQueryException).hasMessageThat().isEqualTo("message");
    assertThat(bigQueryException.getError()).isEqualTo(bigQueryError);
  }

  @Test
  public void testFirstPresent() {
    assertThat(BigQueryUtil.firstPresent(Optional.empty(), Optional.of("a")))
        .isEqualTo(Optional.of("a"));
    assertThat(BigQueryUtil.firstPresent(Optional.empty(), Optional.of("a"), Optional.of("b")))
        .isEqualTo(Optional.of("a"));
    assertThat(BigQueryUtil.firstPresent(Optional.of("a"), Optional.empty()))
        .isEqualTo(Optional.of("a"));
    assertThat(BigQueryUtil.firstPresent(Optional.empty())).isEqualTo(Optional.empty());
  }

  @Test
  public void testSchemaEqualsWithFieldOrder() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());

    assertThat(BigQueryUtil.schemaWritable(s1, s2, true, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true)).isTrue();
  }

  @Test
  public void testSchemaWritableNoFieldOrder() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build(),
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build());

    assertThat(BigQueryUtil.schemaWritable(s1, s2, true, true)).isFalse();
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true)).isTrue();
  }

  @Test
  public void testNullableField() {
    Field f1 = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    Field f2 =
        Field.newBuilder("foo", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build();
    assertThat(BigQueryUtil.fieldWritable(f1, f2, true)).isTrue();
  }

  @Test
  public void testSchemaWritableWithNulls() {
    Schema s =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());
    assertThat(BigQueryUtil.schemaWritable(s, null, false, true)).isFalse();
    // two nulls
    assertThat(BigQueryUtil.schemaWritable(null, null, false, true)).isTrue();
  }

  @Test
  public void testFieldWritableWithNulls() {
    Field f = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    assertThat(BigQueryUtil.fieldWritable(f, null, true)).isFalse();
    // two nulls
    assertThat(BigQueryUtil.fieldWritable(null, null, true)).isTrue();
  }

  @Test
  public void testFieldWritable() {
    Field src =
        Field.newBuilder("foo", StandardSQLTypeName.INT64)
            .setDescription("desc1")
            .setScale(1L)
            .setPrecision(2L)
            .build();
    Field dest =
        Field.newBuilder("foo", StandardSQLTypeName.INT64).setScale(2L).setPrecision(2L).build();
    assertThat(BigQueryUtil.fieldWritable(src, dest, true)).isTrue();
  }

  @Test
  public void testFieldWritableMaxLength() {
    Field f1 = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMaxLength(1L).build();
    Field f2 = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMaxLength(2L).build();
    Field f3 = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMaxLength(3L).build();
    Field f4 = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    assertThat(BigQueryUtil.fieldWritable(f1, f2, true)).isTrue();
    assertThat(BigQueryUtil.fieldWritable(f3, f2, true)).isFalse();
    assertThat(BigQueryUtil.fieldWritable(f3, f4, true)).isFalse();
    assertThat(BigQueryUtil.fieldWritable(f4, f2, true)).isFalse();
  }

  @Test
  public void testSchemaWritableWithEnableModeCheckForSchemaFields() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REQUIRED).build());
    Schema s3 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REPEATED).build());

    assertThat(BigQueryUtil.schemaWritable(s1, s1, false, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s2, s2, false, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s3, s3, false, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s1, s3, false, true)).isFalse();
    assertThat(BigQueryUtil.schemaWritable(s2, s3, false, true)).isFalse();
    assertThat(BigQueryUtil.schemaWritable(s2, s1, false, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s3, s1, false, true)).isFalse();
    assertThat(BigQueryUtil.schemaWritable(s3, s2, false, true)).isFalse();
  }

  @Test
  public void testSchemaWritableWithDisableNullableFieldCheck() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REQUIRED).build());
    Schema s3 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REPEATED).build());

    assertThat(BigQueryUtil.schemaWritable(s1, s1, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s2, s2, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s3, s3, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s1, s3, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s2, s3, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s2, s1, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s3, s1, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s3, s2, false, false)).isTrue();
  }

  @Test
  public void testSchemaWritableWithMoreUnEqualNumberOfFields() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo1", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build(),
            Field.newBuilder("foo2", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("foo1", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build(),
            Field.newBuilder("foo2", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build(),
            Field.newBuilder("foo3", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    Schema s3 =
        Schema.of(
            Field.newBuilder("foo1", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());

    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true)).isTrue();
    assertThat(BigQueryUtil.schemaWritable(s1, s3, false, true)).isFalse();
    assertThat(BigQueryUtil.schemaWritable(s3, s2, false, true)).isTrue();
  }

  @Test
  public void testIsModeWritable() {
    assertThat(BigQueryUtil.isModeWritable(Mode.NULLABLE, Mode.NULLABLE)).isTrue();
    assertThat(BigQueryUtil.isModeWritable(Mode.NULLABLE, Mode.REQUIRED)).isTrue();
    assertThat(BigQueryUtil.isModeWritable(Mode.NULLABLE, Mode.REPEATED)).isFalse();
    assertThat(BigQueryUtil.isModeWritable(Mode.REQUIRED, Mode.NULLABLE)).isTrue();
    assertThat(BigQueryUtil.isModeWritable(Mode.REQUIRED, Mode.REQUIRED)).isTrue();
    assertThat(BigQueryUtil.isModeWritable(Mode.REQUIRED, Mode.REPEATED)).isFalse();
    assertThat(BigQueryUtil.isModeWritable(Mode.REPEATED, Mode.NULLABLE)).isFalse();
    assertThat(BigQueryUtil.isModeWritable(Mode.REPEATED, Mode.REQUIRED)).isFalse();
    assertThat(BigQueryUtil.isModeWritable(Mode.REPEATED, Mode.REPEATED)).isTrue();
  }

  @Test
  public void testCreateVerifiedInstanceNoClass() {
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryUtil.createVerifiedInstance("no.such.Class", String.class));
  }

  @Test
  public void testCreateVerifiedInstanceFailedInheritance() {
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryUtil.createVerifiedInstance("java.lang.String", Map.class));
  }

  @Test
  public void testCreateVerifiedInstance() {
    List result = BigQueryUtil.createVerifiedInstance("java.util.ArrayList", List.class);
    assertThat(result).isNotNull();
    assertThat(result).isEmpty();
  }

  @Test
  public void testCreateVerifiedInstanceWithArg() {
    String result = BigQueryUtil.createVerifiedInstance("java.lang.String", String.class, "test");
    assertThat(result).isNotNull();
    assertThat(result).isEqualTo("test");
  }

  @Test
  public void testVerifySerialization() {
    int[] source = new int[] {1, 2, 3};
    int[] copy = BigQueryUtil.verifySerialization(source);
    assertThat(copy).isEqualTo(source);
  }

  @Test
  public void testVerifySerializationFail() {
    final Object notSerializable = new Object();
    assertThrows(
        IllegalArgumentException.class, () -> BigQueryUtil.verifySerialization(notSerializable));
  }

  @Test
  public void testGetStreamNames() {
    List<String> streamNames =
        BigQueryUtil.getStreamNames(
            ReadSession.newBuilder().addStreams(ReadStream.newBuilder().setName("0")).build());
    assertThat(streamNames).isEqualTo(Arrays.asList("0"));
    streamNames =
        BigQueryUtil.getStreamNames(
            ReadSession.newBuilder()
                .addStreams(ReadStream.newBuilder().setName("0"))
                .addStreams(ReadStream.newBuilder().setName("1"))
                .build());
    assertThat(streamNames).hasSize(2);
    assertThat(streamNames).containsAnyIn(Arrays.asList("0", "1"));
  }

  @Test
  public void testEmptyGetStreamNames() {
    TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    List<String> streamNames =
        BigQueryUtil.getStreamNames(
            ReadSession.newBuilder().setName("abc").setReadOptions(tableReadOptions).build());
    assertThat(streamNames).hasSize(0);
    streamNames = BigQueryUtil.getStreamNames(null);
    assertThat(streamNames).hasSize(0);
  }

  @Test
  public void testGetPartitionField_not_standard_table() {
    TableInfo info = TableInfo.of(TableId.of("foo", "bar"), ViewDefinition.of("Select 1 as test"));
    assertThat(BigQueryUtil.getPartitionFields(info)).isEmpty();
  }

  @Test
  public void testGetPartitionField_no_partitioning() {
    TableInfo info =
        TableInfo.of(TableId.of("foo", "bar"), StandardTableDefinition.newBuilder().build());
    assertThat(BigQueryUtil.getPartitionFields(info)).isEmpty();
  }

  @Test
  public void testGetPartitionField_time_partitioning() {
    TableInfo info =
        TableInfo.of(
            TableId.of("foo", "bar"),
            StandardTableDefinition.newBuilder()
                .setTimePartitioning(
                    TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("test").build())
                .build());
    List<String> partitionFields = BigQueryUtil.getPartitionFields(info);
    assertThat(partitionFields).hasSize(1);
    assertThat(partitionFields).contains("test");
  }

  @Test
  public void testGetPartitionField_range_partitioning() {
    TableInfo info =
        TableInfo.of(
            TableId.of("foo", "bar"),
            StandardTableDefinition.newBuilder()
                .setRangePartitioning(RangePartitioning.newBuilder().setField("test").build())
                .build());
    List<String> partitionFields = BigQueryUtil.getPartitionFields(info);
    assertThat(partitionFields).hasSize(1);
    assertThat(partitionFields).contains("test");
  }

  @Test
  public void testGetPartitionField_hive_partitioning() {
    TableInfo info =
        TableInfo.of(
            TableId.of("foo", "bar"),
            ExternalTableDefinition.newBuilder(
                    "gs://bucket/path",
                    Schema.of(Field.newBuilder("foo", LegacySQLTypeName.STRING).build()),
                    FormatOptions.csv())
                .setHivePartitioningOptions(
                    HivePartitioningOptions.newBuilder()
                        .setFields(Arrays.asList("f1", "f2"))
                        .build())
                .build());
    List<String> partitionFields = BigQueryUtil.getPartitionFields(info);
    assertThat(partitionFields).hasSize(2);
    assertThat(partitionFields).contains("f1");
    assertThat(partitionFields).contains("f2");
  }

  @Test
  public void testGetClusteringFields_not_standard_table() {
    TableInfo info = TableInfo.of(TableId.of("foo", "bar"), ViewDefinition.of("Select 1 as test"));
    assertThat(BigQueryUtil.getClusteringFields(info)).isEmpty();
  }

  @Test
  public void ttestGetClusteringFields_no_clustering() {
    TableInfo info =
        TableInfo.of(TableId.of("foo", "bar"), StandardTableDefinition.newBuilder().build());
    assertThat(BigQueryUtil.getClusteringFields(info)).isEmpty();
  }

  @Test
  public void testGetClusteringFields_time_partitioning() {
    TableInfo info =
        TableInfo.of(
            TableId.of("foo", "bar"),
            StandardTableDefinition.newBuilder()
                .setClustering(
                    Clustering.newBuilder().setFields(ImmutableList.of("c1", "c2")).build())
                .build());
    ImmutableList<String> clusteringFields = BigQueryUtil.getClusteringFields(info);
    assertThat(clusteringFields).hasSize(2);
    assertThat(clusteringFields).contains("c1");
    assertThat(clusteringFields).contains("c2");
  }

  @Test
  public void testFilterLengthInLimit_no_filter() {
    assertThat(BigQueryUtil.filterLengthInLimit(Optional.empty())).isTrue();
  }

  @Test
  public void testFilterLengthInLimit_small_filter() {
    assertThat(BigQueryUtil.filterLengthInLimit(Optional.of("`foo` > 5"))).isTrue();
  }

  @Test
  public void testFilterLengthInLimit_very_large_filter() {
    String tooLarge =
        IntStream.range(0, 2 + 2 << 20).mapToObj(i -> "a").collect(Collectors.joining());
    assertThat(BigQueryUtil.filterLengthInLimit(Optional.of(tooLarge))).isFalse();
  }

  @Test
  public void testGetPrecision() throws Exception {
    assertThat(
            BigQueryUtil.getPrecision(
                Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).setPrecision(5L).build()))
        .isEqualTo(5);
    assertThat(
            BigQueryUtil.getPrecision(Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).build()))
        .isEqualTo(38);
    assertThat(
            BigQueryUtil.getPrecision(
                Field.newBuilder("foo", StandardSQLTypeName.BIGNUMERIC).build()))
        .isEqualTo(76);
    assertThat(BigQueryUtil.getPrecision(Field.newBuilder("foo", StandardSQLTypeName.BOOL).build()))
        .isEqualTo(-1);
  }

  @Test
  public void testGetScale() throws Exception {
    assertThat(
            BigQueryUtil.getScale(
                Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).setScale(5L).build()))
        .isEqualTo(5);
    assertThat(BigQueryUtil.getScale(Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).build()))
        .isEqualTo(9);
    assertThat(
            BigQueryUtil.getScale(Field.newBuilder("foo", StandardSQLTypeName.BIGNUMERIC).build()))
        .isEqualTo(38);
    assertThat(BigQueryUtil.getScale(Field.newBuilder("foo", StandardSQLTypeName.BOOL).build()))
        .isEqualTo(-1);
  }

  @Test
  public void testAdjustSchemaIfNeeded() {
    Schema wantedSchema =
        Schema.of(
            Field.of("numeric", LegacySQLTypeName.NUMERIC),
            Field.of(
                "record",
                LegacySQLTypeName.RECORD,
                Field.of("subfield", LegacySQLTypeName.STRING)));
    Schema existingTableSchema =
        Schema.of(
            Field.of("numeric", LegacySQLTypeName.BIGNUMERIC),
            Field.of(
                "record",
                LegacySQLTypeName.RECORD,
                Field.of("subfield", LegacySQLTypeName.STRING)));
    Schema adjustedSchema = BigQueryUtil.adjustSchemaIfNeeded(wantedSchema, existingTableSchema);
    assertThat(adjustedSchema.getFields()).hasSize(2);
    FieldList adjustedFields = adjustedSchema.getFields();
    assertThat(adjustedFields.get("numeric").getType()).isEqualTo(LegacySQLTypeName.BIGNUMERIC);
    assertThat(adjustedFields.get("record").getType()).isEqualTo(LegacySQLTypeName.RECORD);
    assertThat(adjustedFields.get("record").getSubFields()).hasSize(1);
    assertThat(adjustedFields.get("record").getSubFields().get(0).getType())
        .isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  public void testAdjustSchemaForNewField() {
    Schema wantedSchema =
        Schema.of(
            Field.of("existing_field", LegacySQLTypeName.NUMERIC),
            Field.of("new_field", LegacySQLTypeName.STRING));
    Schema existingTableSchema =
        Schema.of(Field.of("existing_field", LegacySQLTypeName.BIGNUMERIC));
    Schema adjustedSchema = BigQueryUtil.adjustSchemaIfNeeded(wantedSchema, existingTableSchema);
    assertThat(adjustedSchema.getFields()).hasSize(2);
    FieldList adjustedFields = adjustedSchema.getFields();
    assertThat(adjustedFields.get("existing_field").getType())
        .isEqualTo(LegacySQLTypeName.BIGNUMERIC);
    assertThat(adjustedFields.get("new_field").getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  public void testAdjustField_no_op() {
    Field field = Field.of("f", LegacySQLTypeName.BOOLEAN);
    Field existingField = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    Field adjustedField = BigQueryUtil.adjustField(field, existingField);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.BOOLEAN);
  }

  @Test
  public void testAdjustField_numeric_to_big_numeric() {
    Field field = Field.of("numeric", LegacySQLTypeName.NUMERIC);
    Field existingField = Field.of("numeric", LegacySQLTypeName.BIGNUMERIC);
    Field adjustedField = BigQueryUtil.adjustField(field, existingField);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.BIGNUMERIC);
  }

  @Test
  public void testAdjustFieldRecursive() {
    Field field =
        Field.of(
            "record", LegacySQLTypeName.RECORD, Field.of("subfield", LegacySQLTypeName.STRING));
    Field existingField =
        Field.of(
            "record", LegacySQLTypeName.RECORD, Field.of("subfield", LegacySQLTypeName.STRING));
    Field adjustedField = BigQueryUtil.adjustField(field, existingField);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
    assertThat(adjustedField.getSubFields()).hasSize(1);
    assertThat(adjustedField.getSubFields().get(0).getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  public void testAdjustFieldRecursive_with_bignumeric_conversion() {
    Field field =
        Field.of(
            "record", LegacySQLTypeName.RECORD, Field.of("subfield", LegacySQLTypeName.NUMERIC));
    Field existingField =
        Field.of(
            "record", LegacySQLTypeName.RECORD, Field.of("subfield", LegacySQLTypeName.BIGNUMERIC));
    Field adjustedField = BigQueryUtil.adjustField(field, existingField);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
    assertThat(adjustedField.getSubFields()).hasSize(1);
    assertThat(adjustedField.getSubFields().get(0).getType())
        .isEqualTo(LegacySQLTypeName.BIGNUMERIC);
  }

  @Test
  public void testAdjustField_nullExistingField() {
    Field field = Field.of("f", LegacySQLTypeName.BOOLEAN);
    Field adjustedField = BigQueryUtil.adjustField(field, null);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.BOOLEAN);
  }

  @Test
  public void testAdjustField_nullExistingFieldWithRecordType() {
    Field field =
        Field.of(
            "record", LegacySQLTypeName.RECORD, Field.of("subfield", LegacySQLTypeName.NUMERIC));
    Field adjustedField = BigQueryUtil.adjustField(field, null);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
  }

  @Test
  public void testPrepareQueryForLog_withNewLine() {
    assertThat(BigQueryUtil.prepareQueryForLog("SELECT a\nFROM table", 40))
        .isEqualTo("SELECT a\\nFROM table");
  }

  @Test
  public void testPrepareQueryForLog_withoutNewLine() {
    assertThat(BigQueryUtil.prepareQueryForLog("SELECT a FROM table", 40))
        .isEqualTo("SELECT a FROM table");
  }

  @Test
  public void testPrepareQueryForLog_withTruncating() {
    assertThat(BigQueryUtil.prepareQueryForLog("SELECT a FROM table", 10))
        .isEqualTo("SELECT a F\u2026");
  }
}
