/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class NestedSchemaPruningTest {

  @Test
  public void topLevelPruning_keepsOnlyRequiredFields_inFullSchemaOrder() {
    StructType full =
        new StructType()
            .add("a", DataTypes.IntegerType)
            .add("b", DataTypes.StringType)
            .add("c", DataTypes.LongType);
    // Required in a different order to prove ordering follows the full schema.
    StructType required =
        new StructType().add("c", DataTypes.LongType).add("a", DataTypes.IntegerType);

    StructType effective = NestedSchemaPruning.computeEffectiveReadSchema(full, required);

    assertThat(effective.fieldNames()).asList().containsExactly("a", "c").inOrder();
    assertThat(NestedSchemaPruning.toSelectedFieldPaths(effective)).containsExactly("a", "c");
  }

  @Test
  public void nestedStructPruning_producesDottedLeafPaths() {
    StructType owner =
        new StructType().add("login", DataTypes.StringType).add("id", DataTypes.LongType);
    StructType repository = new StructType().add("url", DataTypes.StringType).add("owner", owner);
    StructType full =
        new StructType().add("url", DataTypes.StringType).add("repository", repository);

    StructType requiredOwner = new StructType().add("login", DataTypes.StringType);
    StructType requiredRepo = new StructType().add("owner", requiredOwner);
    StructType required = new StructType().add("repository", requiredRepo);

    StructType effective = NestedSchemaPruning.computeEffectiveReadSchema(full, required);

    assertThat(effective.fieldNames()).asList().containsExactly("repository");
    StructType effectiveRepo = (StructType) effective.apply("repository").dataType();
    assertThat(effectiveRepo.fieldNames()).asList().containsExactly("owner");
    StructType effectiveOwner = (StructType) effectiveRepo.apply("owner").dataType();
    assertThat(effectiveOwner.fieldNames()).asList().containsExactly("login");
    assertThat(NestedSchemaPruning.toSelectedFieldPaths(effective))
        .containsExactly("repository.owner.login");
    assertThat(NestedSchemaPruning.toSelectedFieldPaths(full, effective))
        .containsExactly("repository.owner.login");
  }

  @Test
  public void unchangedStructSelection_keepsTopLevelPath() {
    StructType repository =
        new StructType().add("url", DataTypes.StringType).add("id", DataTypes.LongType);
    StructType full =
        new StructType().add("url", DataTypes.StringType).add("repository", repository);

    assertThat(NestedSchemaPruning.toSelectedFieldPaths(full, full))
        .containsExactly("url", "repository")
        .inOrder();
  }

  @Test
  public void partiallyPrunedStruct_keepsUnchangedNestedStructWhole() {
    StructType owner =
        new StructType().add("login", DataTypes.StringType).add("id", DataTypes.LongType);
    StructType repository = new StructType().add("url", DataTypes.StringType).add("owner", owner);
    StructType full = new StructType().add("repository", repository);
    StructType requiredRepository = new StructType().add("owner", owner);
    StructType required = new StructType().add("repository", requiredRepository);

    StructType effective = NestedSchemaPruning.computeEffectiveReadSchema(full, required);

    assertThat(NestedSchemaPruning.toSelectedFieldPaths(full, effective))
        .containsExactly("repository.owner");
  }

  @Test
  public void isNotNullOnStruct_keepsSingleLeaf_bugScenario() {
    // Mirrors b/534631726: `repository IS NOT NULL` leaves Spark requesting a single leaf.
    StructType repository =
        new StructType().add("url", DataTypes.StringType).add("id", DataTypes.LongType);
    StructType full =
        new StructType().add("url", DataTypes.StringType).add("repository", repository);

    StructType requiredRepo = new StructType().add("url", DataTypes.StringType);
    StructType required =
        new StructType().add("url", DataTypes.StringType).add("repository", requiredRepo);

    StructType effective = NestedSchemaPruning.computeEffectiveReadSchema(full, required);

    assertThat(effective.fieldNames()).asList().containsExactly("url", "repository").inOrder();
    StructType effectiveRepo = (StructType) effective.apply("repository").dataType();
    assertThat(effectiveRepo.fieldNames()).asList().containsExactly("url");
    assertThat(NestedSchemaPruning.toSelectedFieldPaths(full, effective))
        .containsExactly("url", "repository.url");
  }

  @Test
  public void isNotNullOnStruct_preservesEmptyStructRequestedBySpark4() {
    StructType repository =
        new StructType().add("url", DataTypes.StringType).add("id", DataTypes.LongType);
    StructType full =
        new StructType().add("url", DataTypes.StringType).add("repository", repository);
    StructType required =
        new StructType().add("url", DataTypes.StringType).add("repository", new StructType());

    StructType effective = NestedSchemaPruning.computeEffectiveReadSchema(full, required);

    assertThat(effective.fieldNames()).asList().containsExactly("url", "repository").inOrder();
    assertThat(((StructType) effective.apply("repository").dataType()).isEmpty()).isTrue();
    assertThat(NestedSchemaPruning.toSelectedFieldPaths(full, effective))
        .containsExactly("url", "repository")
        .inOrder();
  }

  @Test
  public void arrayOfStruct_keptWhole_notNestedPruned() {
    StructType element =
        new StructType().add("a", DataTypes.StringType).add("b", DataTypes.StringType);
    StructType full = new StructType().add("payload", DataTypes.createArrayType(element));

    StructType requiredElement = new StructType().add("a", DataTypes.StringType);
    StructType required =
        new StructType().add("payload", DataTypes.createArrayType(requiredElement));

    StructType effective = NestedSchemaPruning.computeEffectiveReadSchema(full, required);

    // The array<struct> field must be kept whole (both a and b), since BigQuery cannot reliably
    // sub-select within repeated records.
    org.apache.spark.sql.types.ArrayType effectiveArray =
        (org.apache.spark.sql.types.ArrayType) effective.apply("payload").dataType();
    StructType effectiveElement = (StructType) effectiveArray.elementType();
    assertThat(effectiveElement.fieldNames()).asList().containsExactly("a", "b").inOrder();
    // Selected field path is the whole array field (no descent into it).
    assertThat(NestedSchemaPruning.toSelectedFieldPaths(effective)).containsExactly("payload");
  }

  @Test
  public void pruneBigQuerySchema_prunesNestedRecordToSelectedLeaf() {
    Field owner =
        Field.newBuilder(
                "owner",
                LegacySQLTypeName.RECORD,
                Field.of("login", LegacySQLTypeName.STRING),
                Field.of("id", LegacySQLTypeName.INTEGER))
            .setMode(Mode.NULLABLE)
            .build();
    Field repository =
        Field.newBuilder(
                "repository",
                LegacySQLTypeName.RECORD,
                Field.of("url", LegacySQLTypeName.STRING),
                owner)
            .setMode(Mode.NULLABLE)
            .build();
    Schema full = Schema.of(Field.of("url", LegacySQLTypeName.STRING), repository);

    Schema pruned =
        NestedSchemaPruning.pruneBigQuerySchema(full, ImmutableSet.of("repository.owner.login"));

    assertThat(pruned.getFields().stream().map(Field::getName)).containsExactly("repository");
    Field prunedRepo = pruned.getFields().get("repository");
    assertThat(prunedRepo.getSubFields().stream().map(Field::getName)).containsExactly("owner");
    Field prunedOwner = prunedRepo.getSubFields().get("owner");
    assertThat(prunedOwner.getSubFields().stream().map(Field::getName)).containsExactly("login");
  }

  @Test
  public void pruneBigQuerySchema_keepsRepeatedRecordWhole() {
    Field payload =
        Field.newBuilder(
                "payload",
                LegacySQLTypeName.RECORD,
                Field.of("a", LegacySQLTypeName.STRING),
                Field.of("b", LegacySQLTypeName.STRING))
            .setMode(Mode.REPEATED)
            .build();
    Schema full = Schema.of(payload);

    Schema pruned = NestedSchemaPruning.pruneBigQuerySchema(full, ImmutableSet.of("payload"));

    Field prunedPayload = pruned.getFields().get("payload");
    assertThat(prunedPayload.getMode()).isEqualTo(Mode.REPEATED);
    assertThat(prunedPayload.getSubFields().stream().map(Field::getName))
        .containsExactly("a", "b")
        .inOrder();
  }
}
