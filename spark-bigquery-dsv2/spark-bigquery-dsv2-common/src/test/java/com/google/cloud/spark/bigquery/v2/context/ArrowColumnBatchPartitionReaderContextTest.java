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
package com.google.cloud.spark.bigquery.v2.context;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.connector.common.ArrowUtil;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArrowColumnBatchPartitionReaderContextTest {

  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = ArrowUtil.newRootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testEnsureStructVectorsHaveChildrenPopulatesChildVectors() throws Exception {
    Field intChildField =
        new Field("int_val", FieldType.nullable(Types.MinorType.INT.getType()), null);
    Field structField =
        new Field(
            "struct_col",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            ImmutableList.of(intChildField));
    Schema schema = new Schema(ImmutableList.of(structField));

    // Create a record batch with full data (struct_col + int_val)
    ArrowRecordBatch recordBatch;
    try (VectorSchemaRoot sourceRoot = VectorSchemaRoot.create(schema, allocator)) {
      StructVector structVec = (StructVector) sourceRoot.getVector("struct_col");
      IntVector intVec =
          structVec.addOrGet(
              "int_val", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
      structVec.allocateNew();
      intVec.set(0, 42);
      structVec.setValueCount(1);
      sourceRoot.setRowCount(1);

      VectorUnloader unloader = new VectorUnloader(sourceRoot);
      recordBatch = unloader.getRecordBatch();
    }

    // Create a target StructVector empty without children initially
    try (StructVector targetStructVec = StructVector.empty("struct_col", allocator);
        ArrowRecordBatch batchToLoad = recordBatch) {
      assertThat(targetStructVec.getChildrenFromFields()).isEmpty();

      // Initialize children matching the schema fields
      targetStructVec.initializeChildrenFromFields(structField.getChildren());
      assertThat(targetStructVec.getChildrenFromFields()).hasSize(1);

      FieldVector targetFieldVec = targetStructVec;
      VectorSchemaRoot targetRoot =
          new VectorSchemaRoot(
              ImmutableList.of(targetFieldVec.getField()), ImmutableList.of(targetFieldVec));

      VectorLoader loader = new VectorLoader(targetRoot);
      loader.load(batchToLoad);

      assertThat(targetRoot.getRowCount()).isEqualTo(1);
      StructVector loadedStruct = (StructVector) targetRoot.getVector("struct_col");
      IntVector loadedInt = (IntVector) loadedStruct.getChild("int_val");
      assertThat(loadedInt.get(0)).isEqualTo(42);
    }
  }
}
