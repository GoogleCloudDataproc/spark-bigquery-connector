/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;

public class GenericBigQueryInputPartitionReader implements Serializable {

  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
  private InternalRow currentRow;

  public GenericBigQueryInputPartitionReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToInternalRowIteratorConverter converter,
      ReadRowsHelper readRowsHelper) {
    this.readRowsResponses = readRowsResponses;
    this.converter = converter;
    this.readRowsHelper = readRowsHelper;
  }

  public Iterator<ReadRowsResponse> getReadRowsResponses() {
    return readRowsResponses;
  }

  public ReadRowsResponseToInternalRowIteratorConverter getConverter() {
    return converter;
  }

  public ReadRowsHelper getReadRowsHelper() {
    return readRowsHelper;
  }

  public Iterator<InternalRow> getRows() {
    return rows;
  }

  public InternalRow getCurrentRow() {
    return currentRow;
  }

  public boolean next() {
    while (!rows.hasNext()) {
      if (!this.readRowsResponses.hasNext()) {
        return false;
      }
      ReadRowsResponse readRowsResponse = this.readRowsResponses.next();
      rows = this.converter.convert(readRowsResponse);
    }
    currentRow = rows.next();
    return true;
  }
}
