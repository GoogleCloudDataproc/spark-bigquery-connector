/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package org.apache.spark.sql;

import static com.google.common.truth.Truth.assertThat;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;

public class Scala213SparkSqlUtilsTest {

  @Test
  public void testRowToInternalRow() throws Exception {
    SparkSqlUtils ssu = SparkSqlUtils.getInstance();
    assertThat(ssu).isInstanceOf(Scala213SparkSqlUtils.class);
    Row row = new GenericRow(new Object[] {UTF8String.fromString("a"), 1});
    InternalRow internalRow = ssu.rowToInternalRow(row);
    assertThat(internalRow.numFields()).isEqualTo(2);
    assertThat(internalRow.getString(0).toString()).isEqualTo("a");
    assertThat(internalRow.getInt(1)).isEqualTo(1);
  }
}
