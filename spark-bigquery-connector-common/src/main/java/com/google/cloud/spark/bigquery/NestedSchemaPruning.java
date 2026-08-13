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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Helpers for honoring Spark's nested (struct) schema pruning in the BigQuery DataSourceV2 reader.
 *
 * <p>Spark 3.5 prunes struct sub-fields in the logical plan and passes the pruned schema to {@code
 * SupportsPushDownRequiredColumns.pruneColumns}. If the scan then reports a wider schema via {@code
 * readSchema()} than what BigQuery actually returns, Spark 3.5's stricter DataSourceV2 output
 * validation throws a schema-resolution error (b/534631726). These helpers compute the schema the
 * connector will actually read (so {@code readSchema()} matches BigQuery's wire output) and the
 * dotted {@code selected_fields} paths that make BigQuery return exactly that schema.
 *
 * <p>Pruning is applied inside {@link StructType} only. {@code ARRAY<STRUCT>} (repeated records),
 * {@code MAP} and scalar fields are kept whole, because the BigQuery Storage Read API cannot
 * reliably sub-select fields within repeated records; Spark applies any remaining pruning for those
 * types in-engine via a Project.
 */
public final class NestedSchemaPruning {

  private NestedSchemaPruning() {}

  /**
   * Computes the schema the connector will actually read from BigQuery given the schema Spark
   * requires.
   *
   * <p>Field order follows {@code fullSchema} (and, recursively, the full struct definitions),
   * matching the BigQuery Storage Read API which orders output by the table schema rather than by
   * the order of {@code selected_fields}.
   *
   * @param fullSchema the connector's complete (unpruned) read schema
   * @param requiredSchema the schema Spark requested via {@code pruneColumns}
   * @return the effective schema BigQuery will return for the derived {@code selected_fields}
   */
  public static StructType computeEffectiveReadSchema(
      StructType fullSchema, StructType requiredSchema) {
    Set<String> requiredNames = new HashSet<>();
    for (String name : requiredSchema.fieldNames()) {
      requiredNames.add(name);
    }
    StructType result = new StructType();
    for (StructField fullField : fullSchema.fields()) {
      if (!requiredNames.contains(fullField.name())) {
        continue;
      }
      StructField requiredField = requiredSchema.apply(fullField.name());
      result = result.add(pruneField(fullField, requiredField));
    }
    return result;
  }

  private static StructField pruneField(StructField fullField, StructField requiredField) {
    DataType fullType = fullField.dataType();
    DataType requiredType = requiredField.dataType();
    if (fullType instanceof StructType && requiredType instanceof StructType) {
      StructType prunedChildren =
          computeEffectiveReadSchema((StructType) fullType, (StructType) requiredType);
      // Defensive: never emit an empty struct (which would drop the field from selected_fields and
      // desync readSchema from the wire). Fall back to the full struct definition.
      if (prunedChildren.isEmpty()) {
        return fullField;
      }
      return new StructField(
          fullField.name(), prunedChildren, fullField.nullable(), fullField.metadata());
    }
    // scalar, array (including ARRAY<STRUCT>), map: keep the full definition.
    return fullField;
  }

  /**
   * Flattens an (already effective) schema into BigQuery {@code selected_fields} paths. Descends
   * only into {@link StructType} fields, producing dot-joined leaf paths; arrays, maps and scalars
   * contribute a single path so BigQuery returns them whole.
   */
  public static ImmutableList<String> toSelectedFieldPaths(StructType schema) {
    List<String> paths = new ArrayList<>();
    collectPaths(schema, "", paths);
    return ImmutableList.copyOf(paths);
  }

  /**
   * Converts an effective read schema into BigQuery {@code selected_fields}, keeping unchanged
   * structs selected as a whole and using dotted paths only where Spark actually pruned children.
   *
   * @param fullSchema the schema before Spark's column pruning
   * @param effectiveSchema the schema after Spark's column pruning
   */
  public static ImmutableList<String> toSelectedFieldPaths(
      StructType fullSchema, StructType effectiveSchema) {
    List<String> paths = new ArrayList<>();
    collectComparedPaths(fullSchema, effectiveSchema, "", paths);
    return ImmutableList.copyOf(paths);
  }

  private static void collectPaths(StructType schema, String prefix, List<String> paths) {
    for (StructField field : schema.fields()) {
      String path = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
      if (field.dataType() instanceof StructType && !((StructType) field.dataType()).isEmpty()) {
        collectPaths((StructType) field.dataType(), path, paths);
      } else {
        paths.add(path);
      }
    }
  }

  private static void collectComparedPaths(
      StructType fullSchema, StructType effectiveSchema, String prefix, List<String> paths) {
    Set<String> fullNames = new HashSet<>();
    for (String name : fullSchema.fieldNames()) {
      fullNames.add(name);
    }
    for (StructField effectiveField : effectiveSchema.fields()) {
      String path = prefix.isEmpty() ? effectiveField.name() : prefix + "." + effectiveField.name();
      if (!fullNames.contains(effectiveField.name())) {
        collectFieldPaths(effectiveField, path, paths);
        continue;
      }
      StructField fullField = fullSchema.apply(effectiveField.name());
      DataType fullType = fullField.dataType();
      DataType effectiveType = effectiveField.dataType();
      if (fullType.equals(effectiveType) || !(effectiveType instanceof StructType)) {
        paths.add(path);
      } else if (fullType instanceof StructType) {
        collectComparedPaths((StructType) fullType, (StructType) effectiveType, path, paths);
      } else {
        collectFieldPaths(effectiveField, path, paths);
      }
    }
  }

  private static void collectFieldPaths(StructField field, String path, List<String> paths) {
    if (field.dataType() instanceof StructType && !((StructType) field.dataType()).isEmpty()) {
      collectPaths((StructType) field.dataType(), path, paths);
    } else {
      paths.add(path);
    }
  }

  /**
   * Prunes a BigQuery {@link Schema} so it matches the given set of dotted {@code selected_fields}
   * paths. Non-repeated {@code RECORD} fields are pruned to the selected sub-fields; repeated
   * records (arrays of structs), scalars and any field selected as a whole are kept intact. Used to
   * keep the AVRO read path's BigQuery schema consistent with the pruned wire schema.
   *
   * @param fullSchema the complete BigQuery schema
   * @param selectedPaths dotted field paths (e.g. {@code "repository.url"}) as sent to the Storage
   *     Read API
   * @return a pruned BigQuery schema
   */
  public static Schema pruneBigQuerySchema(Schema fullSchema, Set<String> selectedPaths) {
    return Schema.of(pruneFields(fullSchema.getFields(), selectedPaths, ""));
  }

  private static FieldList pruneFields(FieldList fields, Set<String> selectedPaths, String prefix) {
    List<Field> result = new ArrayList<>();
    for (Field field : fields) {
      String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
      boolean wholeSelected = selectedPaths.contains(path);
      boolean subSelected = hasSelectedDescendant(selectedPaths, path);
      if (!wholeSelected && !subSelected) {
        continue;
      }
      boolean isNonRepeatedRecord =
          LegacySQLTypeName.RECORD.equals(field.getType())
              && field.getMode() != Field.Mode.REPEATED;
      if (isNonRepeatedRecord && !wholeSelected) {
        FieldList prunedSubFields = pruneFields(field.getSubFields(), selectedPaths, path);
        result.add(field.toBuilder().setType(LegacySQLTypeName.RECORD, prunedSubFields).build());
      } else {
        // scalar, repeated record (ARRAY<STRUCT>), or a struct selected as a whole.
        result.add(field);
      }
    }
    return FieldList.of(result);
  }

  private static boolean hasSelectedDescendant(Set<String> selectedPaths, String path) {
    String prefix = path + ".";
    for (String selectedPath : selectedPaths) {
      if (selectedPath.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
