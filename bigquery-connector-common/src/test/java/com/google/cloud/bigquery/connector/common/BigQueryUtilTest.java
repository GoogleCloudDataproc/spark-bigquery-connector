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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigLakeConfiguration;
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

  private static void checkFailureMessage(ComparisonResult result, String message) {
    assertThat(result.valuesAreEqual()).isFalse();
    assertThat(result.makeMessage()).isEqualTo(message);
  }

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

    assertThat(BigQueryUtil.schemaWritable(s1, s2, true, true)).isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true))
        .isEqualTo(ComparisonResult.equal());
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

    assertThat(BigQueryUtil.schemaWritable(s1, s2, true, true))
        .isEqualTo(ComparisonResult.differentNoDescription());
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true))
        .isEqualTo(ComparisonResult.equal());
  }

  @Test
  public void testNullableField() {
    Field f1 = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    Field f2 =
        Field.newBuilder("foo", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build();
    assertThat(BigQueryUtil.fieldWritable(f1, f2, true)).isEqualTo(ComparisonResult.equal());
  }

  @Test
  public void testSchemaWritableWithNulls() {
    Schema s =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());
    assertThat(BigQueryUtil.schemaWritable(s, null, false, true))
        .isEqualTo(ComparisonResult.differentNoDescription());
    // two nulls
    assertThat(BigQueryUtil.schemaWritable(null, null, false, true))
        .isEqualTo(ComparisonResult.equal());
  }

  @Test
  public void testFieldWritableWithNulls() {
    Field f = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    checkFailureMessage(
        BigQueryUtil.fieldWritable(f, null, true), "Field not found in destination: foo");
    // two nulls
    assertThat(BigQueryUtil.fieldWritable(null, null, true)).isEqualTo(ComparisonResult.equal());
  }

  @Test
  public void testRequiredFieldNotFound() {
    Field f = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMode(Mode.REQUIRED).build();
    checkFailureMessage(
        BigQueryUtil.fieldWritable(null, f, true), "Required field not found in source: foo");
  }

  @Test
  public void testFieldNameMismatch() {
    Field src = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    Field dest = Field.newBuilder("bar", StandardSQLTypeName.INT64).build();
    checkFailureMessage(
        BigQueryUtil.fieldWritable(src, dest, true),
        "Wrong field name, expected source field name: bar but was: foo");
  }

  @Test
  public void testSubfieldsMismatch() {
    Field src =
        Field.of(
            "fooRecord",
            LegacySQLTypeName.RECORD,
            Field.of("subfield1", LegacySQLTypeName.STRING),
            Field.of("subfield2", LegacySQLTypeName.STRING));
    Field dest =
        Field.of(
            "fooRecord", LegacySQLTypeName.RECORD, Field.of("subfield1", LegacySQLTypeName.STRING));
    checkFailureMessage(
        BigQueryUtil.fieldWritable(src, dest, true),
        "Subfields mismatch for: fooRecord, Number of source fields: 2 is larger than number of destination fields: 1");
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
    assertThat(BigQueryUtil.fieldWritable(src, dest, true)).isEqualTo(ComparisonResult.equal());
  }

  @Test
  public void testFieldWritable_notTypeWritable() {
    Field src = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    Field dest = Field.newBuilder("foo", StandardSQLTypeName.DATE).build();
    checkFailureMessage(
        BigQueryUtil.fieldWritable(src, dest, true),
        "Incompatible type for field: foo, cannot write source type: INTEGER to destination type: DATE");
  }

  @Test
  public void testFieldWritableMaxLength() {
    Field f1 = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMaxLength(1L).build();
    Field f2 = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMaxLength(2L).build();
    Field f3 = Field.newBuilder("foo", StandardSQLTypeName.INT64).setMaxLength(3L).build();
    Field f4 = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    assertThat(BigQueryUtil.fieldWritable(f1, f2, true)).isEqualTo(ComparisonResult.equal());
    checkFailureMessage(
        BigQueryUtil.fieldWritable(f3, f2, true),
        "Incompatible max length for field: foo, cannot write source field with max length: 3 to destination field with max length: 2");
    checkFailureMessage(
        BigQueryUtil.fieldWritable(f3, f4, true),
        "Incompatible max length for field: foo, cannot write source field with max length: 3 to destination field with max length: null");
    checkFailureMessage(
        BigQueryUtil.fieldWritable(f4, f2, true),
        "Incompatible max length for field: foo, cannot write source field with max length: null to destination field with max length: 2");
  }

  @Test
  public void testFieldWritableScaleAndPrecision() {
    Field s5p5 =
        Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).setScale(5L).setPrecision(5L).build();
    Field s3p7 =
        Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).setScale(3L).setPrecision(7L).build();
    Field s7p3 =
        Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).setScale(7L).setPrecision(3L).build();
    Field s3p3 =
        Field.newBuilder("foo", StandardSQLTypeName.NUMERIC).setScale(3L).setPrecision(3L).build();
    checkFailureMessage(
        BigQueryUtil.fieldWritable(s5p5, s3p7, true),
        "Incompatible scale, precision for field: foo, cannot write source field with scale, precision: (5, 5) to destination field with scale, precision: (3, 7)");
    checkFailureMessage(
        BigQueryUtil.fieldWritable(s5p5, s7p3, true),
        "Incompatible scale, precision for field: foo, cannot write source field with scale, precision: (5, 5) to destination field with scale, precision: (7, 3)");
    checkFailureMessage(
        BigQueryUtil.fieldWritable(s5p5, s3p3, true),
        "Incompatible scale, precision for field: foo, cannot write source field with scale, precision: (5, 5) to destination field with scale, precision: (3, 3)");
    assertThat(BigQueryUtil.fieldWritable(s3p3, s5p5, true)).isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.fieldWritable(s3p3, s3p7, true)).isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.fieldWritable(s3p3, s7p3, true)).isEqualTo(ComparisonResult.equal());
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

    assertThat(BigQueryUtil.schemaWritable(s1, s1, false, true))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s2, s2, false, true))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s3, s3, false, true))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true))
        .isEqualTo(ComparisonResult.equal());
    checkFailureMessage(
        BigQueryUtil.schemaWritable(s1, s3, false, true),
        "Incompatible mode for field: foo, cannot write source field mode: NULLABLE to destination field mode: REPEATED");
    checkFailureMessage(
        BigQueryUtil.schemaWritable(s2, s3, false, true),
        "Incompatible mode for field: foo, cannot write source field mode: REQUIRED to destination field mode: REPEATED");
    assertThat(BigQueryUtil.schemaWritable(s2, s1, false, true))
        .isEqualTo(ComparisonResult.equal());
    checkFailureMessage(
        BigQueryUtil.schemaWritable(s3, s1, false, true),
        "Incompatible mode for field: foo, cannot write source field mode: REPEATED to destination field mode: NULLABLE");
    checkFailureMessage(
        BigQueryUtil.schemaWritable(s3, s2, false, true),
        "Incompatible mode for field: foo, cannot write source field mode: REPEATED to destination field mode: REQUIRED");
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

    assertThat(BigQueryUtil.schemaWritable(s1, s1, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s2, s2, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s3, s3, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s1, s3, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s2, s3, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s2, s1, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s3, s1, false, false))
        .isEqualTo(ComparisonResult.equal());
    assertThat(BigQueryUtil.schemaWritable(s3, s2, false, false))
        .isEqualTo(ComparisonResult.equal());
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

    assertThat(BigQueryUtil.schemaWritable(s1, s2, false, true))
        .isEqualTo(ComparisonResult.equal());
    checkFailureMessage(
        BigQueryUtil.schemaWritable(s1, s3, false, true),
        "Number of source fields: 2 is larger than number of destination fields: 1");
    assertThat(BigQueryUtil.schemaWritable(s3, s2, false, true))
        .isEqualTo(ComparisonResult.equal());
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
  public void testGetPartitionField_time_partitioning_pseudoColumn() {
    TableInfo info =
        TableInfo.of(
            TableId.of("foo", "bar"),
            StandardTableDefinition.newBuilder()
                .setTimePartitioning(
                    TimePartitioning.newBuilder(TimePartitioning.Type.HOUR).build())
                .build());
    List<String> partitionFields = BigQueryUtil.getPartitionFields(info);
    assertThat(partitionFields).hasSize(1);
    assertThat(partitionFields).contains("_PARTITIONTIME");
  }

  @Test
  public void testGetPartitionField_time_partitioning_pseudoColumn_day() {
    TableInfo info =
        TableInfo.of(
            TableId.of("foo", "bar"),
            StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build())
                .build());
    List<String> partitionFields = BigQueryUtil.getPartitionFields(info);
    assertThat(partitionFields).hasSize(2);
    assertThat(partitionFields).contains("_PARTITIONTIME");
    assertThat(partitionFields).contains("_PARTITIONDATE");
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
    Schema adjustedSchema =
        BigQueryUtil.adjustSchemaIfNeeded(wantedSchema, existingTableSchema, false);
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
    Schema adjustedSchema =
        BigQueryUtil.adjustSchemaIfNeeded(wantedSchema, existingTableSchema, false);
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
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, false);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.BIGNUMERIC);
  }

  @Test
  public void testAdjustField_numeric_to_big_numeric() {
    Field field = Field.of("numeric", LegacySQLTypeName.NUMERIC);
    Field existingField = Field.of("numeric", LegacySQLTypeName.BIGNUMERIC);
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, false);
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
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, false);
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
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, false);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
    assertThat(adjustedField.getSubFields()).hasSize(1);
    assertThat(adjustedField.getSubFields().get(0).getType())
        .isEqualTo(LegacySQLTypeName.BIGNUMERIC);
  }

  @Test
  public void testAdjustField_nullExistingField() {
    Field field = Field.of("f", LegacySQLTypeName.BOOLEAN);
    Field adjustedField = BigQueryUtil.adjustField(field, null, false);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.BOOLEAN);
  }

  @Test
  public void testAdjustField_nullExistingFieldWithRecordType() {
    Field field =
        Field.of(
            "record", LegacySQLTypeName.RECORD, Field.of("subfield", LegacySQLTypeName.NUMERIC));
    Field adjustedField = BigQueryUtil.adjustField(field, null, false);
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

  @Test
  public void testSanitizeLabelValue() {
    // testing to lower case transformation, and character handling
    assertThat(BigQueryUtil.sanitizeLabelValue("Foo-bar*")).isEqualTo("foo-bar_");
    // testing Strings longer than 63 characters
    assertThat(
            BigQueryUtil.sanitizeLabelValue(
                "1234567890123456789012345678901234567890123456789012345678901234567890"))
        .isEqualTo("123456789012345678901234567890123456789012345678901234567890123");
  }

  @Test
  public void testIsBigLakeManagedTable_with_BigLakeManagedTable() {
    TableInfo bigLakeManagedTable =
        TableInfo.of(
            TableId.of("dataset", "biglakemanagedtable"),
            StandardTableDefinition.newBuilder()
                .setBigLakeConfiguration(
                    BigLakeConfiguration.newBuilder()
                        .setTableFormat("ICEBERG")
                        .setConnectionId("us-connection")
                        .setFileFormat("PARQUET")
                        .setStorageUri("gs://bigquery/blmt/nations.parquet")
                        .build())
                .build());

    assertTrue(BigQueryUtil.isBigLakeManagedTable(bigLakeManagedTable));
  }

  @Test
  public void testIsBigLakeManagedTable_with_BigQueryExternalTable() {
    TableInfo bigQueryExternalTable =
        TableInfo.of(
            TableId.of("dataset", "bigqueryexternaltable"),
            ExternalTableDefinition.newBuilder(
                    "gs://bigquery/nations.parquet", FormatOptions.avro())
                .build());

    assertFalse(BigQueryUtil.isBigLakeManagedTable(bigQueryExternalTable));
  }

  @Test
  public void testIsBigLakeManagedTable_with_BigQueryNativeTable() {
    TableInfo bigQueryNativeTable =
        TableInfo.of(
            TableId.of("dataset", "bigquerynativetable"),
            StandardTableDefinition.newBuilder().setLocation("us-east-1").build());

    assertFalse(BigQueryUtil.isBigLakeManagedTable(bigQueryNativeTable));
  }

  @Test
  public void testIsBigQueryNativeTable_with_BigLakeManagedTable() {
    TableInfo bigLakeManagedTable =
        TableInfo.of(
            TableId.of("dataset", "biglakemanagedtable"),
            StandardTableDefinition.newBuilder()
                .setBigLakeConfiguration(
                    BigLakeConfiguration.newBuilder()
                        .setTableFormat("ICEBERG")
                        .setConnectionId("us-connection")
                        .setFileFormat("PARQUET")
                        .setStorageUri("gs://bigquery/blmt/nations.parquet")
                        .build())
                .build());

    assertFalse(BigQueryUtil.isBigQueryNativeTable(bigLakeManagedTable));
  }

  @Test
  public void testIsBigQueryNativeTable_with_BigQueryExternalTable() {
    TableInfo bigQueryExternalTable =
        TableInfo.of(
            TableId.of("dataset", "bigqueryexternaltable"),
            ExternalTableDefinition.newBuilder(
                    "gs://bigquery/nations.parquet", FormatOptions.avro())
                .build());

    assertFalse(BigQueryUtil.isBigQueryNativeTable(bigQueryExternalTable));
  }

  @Test
  public void testIsBigQueryNativeTable_with_BigQueryNativeTable() {
    TableInfo bigQueryNativeTable =
        TableInfo.of(
            TableId.of("dataset", "bigquerynativetable"),
            StandardTableDefinition.newBuilder().setLocation("us-east-1").build());

    assertTrue(BigQueryUtil.isBigQueryNativeTable(bigQueryNativeTable));
  }

  @Test
  public void testAdjustField_nullable_allowRelaxation() {
    Field field = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    field = field.toBuilder().setMode(Mode.NULLABLE).build();
    Field existingField = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    existingField = existingField.toBuilder().setMode(Mode.REQUIRED).build();
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, true);
    assertThat(adjustedField.getMode()).isEqualTo(Mode.NULLABLE);
  }

  @Test
  public void testAdjustField_exisitingFieldNullable_allowRelaxation() {
    Field field = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    field = field.toBuilder().setMode(Mode.REQUIRED).build();
    Field existingField = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    existingField = existingField.toBuilder().setMode(Mode.NULLABLE).build();
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, true);
    assertThat(adjustedField.getMode()).isEqualTo(Mode.NULLABLE);
  }

  @Test
  public void testAdjustField_nullable_dontAllowRelaxation() {
    Field field = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    field = field.toBuilder().setMode(Mode.NULLABLE).build();
    Field existingField = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    existingField = existingField.toBuilder().setMode(Mode.REQUIRED).build();
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, false);
    assertThat(adjustedField.getMode()).isEqualTo(Mode.REQUIRED);
  }

  @Test
  public void testAdjustField_numeric_to_bigNumeric() {
    Field field = Field.of("f", LegacySQLTypeName.NUMERIC);
    field = field.toBuilder().setMode(Mode.NULLABLE).build();
    Field existingField = Field.of("f", LegacySQLTypeName.BIGNUMERIC);
    existingField = existingField.toBuilder().setMode(Mode.REQUIRED).build();
    Field adjustedField = BigQueryUtil.adjustField(field, existingField, false);
    assertThat(adjustedField.getType()).isEqualTo(LegacySQLTypeName.BIGNUMERIC);
  }
}
