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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    assertThat(BigQueryUtil.schemaEquals(s1, s2, true, true)).isTrue();
    assertThat(BigQueryUtil.schemaEquals(s1, s2, false, true)).isTrue();
  }

  @Test
  public void testSchemaEqualsNoFieldOrder() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build(),
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build());

    assertThat(BigQueryUtil.schemaEquals(s1, s2, true, true)).isFalse();
    assertThat(BigQueryUtil.schemaEquals(s1, s2, false, true)).isTrue();
  }

  @Test
  public void testNullableField() {
    Field f1 = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    Field f2 =
        Field.newBuilder("foo", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build();
    assertThat(BigQueryUtil.fieldEquals(f1, f2, true)).isTrue();
  }

  @Test
  public void testSchemaEqualsWithNulls() {
    Schema s =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.INT64).build(),
            Field.newBuilder("bar", StandardSQLTypeName.STRING).build());
    assertThat(BigQueryUtil.schemaEquals(s, null, false, true)).isFalse();
    // two nulls
    assertThat(BigQueryUtil.schemaEquals(null, null, false, true)).isTrue();
  }

  @Test
  public void testFieldEqualsWithNulls() {
    Field f = Field.newBuilder("foo", StandardSQLTypeName.INT64).build();
    assertThat(BigQueryUtil.fieldEquals(f, null, true)).isFalse();
    // two nulls
    assertThat(BigQueryUtil.fieldEquals(null, null, true)).isTrue();
  }

  @Test
  public void testSchemaEqualsWithEnableModeCheckForSchemaFields() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REQUIRED).build());
    Schema s3 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REPEATED).build());

    assertThat(BigQueryUtil.schemaEquals(s1, s2, false, true)).isFalse();
    assertThat(BigQueryUtil.schemaEquals(s1, s3, false, true)).isFalse();
    assertThat(BigQueryUtil.schemaEquals(s2, s3, false, true)).isFalse();
  }

  @Test
  public void testSchemaEqualsWithDisableNullableFieldCheck() {
    Schema s1 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    Schema s2 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REQUIRED).build());
    Schema s3 =
        Schema.of(
            Field.newBuilder("foo", StandardSQLTypeName.STRING).setMode(Mode.REPEATED).build());

    assertThat(BigQueryUtil.schemaEquals(s1, s2, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaEquals(s1, s3, false, false)).isTrue();
    assertThat(BigQueryUtil.schemaEquals(s2, s3, false, false)).isTrue();
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
}
