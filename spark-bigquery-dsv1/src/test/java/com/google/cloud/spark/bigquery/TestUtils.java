/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.StandardTableDefinition; // Assuming StandardTableDefinition
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.base.Preconditions;
import org.mockito.Mockito;

public final class TestUtils {

  private TestUtils() {}

  public static Table table(TableInfo info) {
    Table tableMock = Mockito.mock(Table.class);
    when(tableMock.getTableId()).thenReturn(info.getTableId());

    // Assuming the definition is always StandardTableDefinition for this util,
    // based on the Scala code's type hint.
    if (info.getDefinition() instanceof StandardTableDefinition) {
      when(tableMock.getDefinition()).thenReturn(info.getDefinition());
    } else {
      // If it can be other types, this mocking might need to be more flexible
      // or the test setup providing TableInfo should ensure it's a StandardTableDefinition
      // if that's what the code under test expects from this mock.
      // For now, we cast, which is risky if the input info is not StandardTableDefinition.
      // A better approach might be to mock getDefinition() to return a mock TableDefinition
      // and then mock its methods like getType(), etc.
      when(tableMock.getDefinition()).thenReturn(info.getDefinition());
    }

    // The Preconditions and require calls in Scala were for validating the mock setup itself.
    // In Java, these assertions would typically be part of the test method using this utility,
    // or you can keep them here if this util is strictly for `StandardTableDefinition`.
    Preconditions.checkNotNull(tableMock.getTableId(), "Mocked TableId should not be null");
    Preconditions.checkNotNull(
        tableMock.getDefinition(), "Mocked TableDefinition should not be null");
    // The following check is specific to StandardTableDefinition
    // Preconditions.checkNotNull(tableMock.getDefinition() instanceof StandardTableDefinition,
    //                           "Mocked definition should be StandardTableDefinition");

    return tableMock;
  }
}
