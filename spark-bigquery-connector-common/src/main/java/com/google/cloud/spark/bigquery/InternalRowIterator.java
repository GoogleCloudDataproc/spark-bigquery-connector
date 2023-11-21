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

package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iterator on InternalRow that wraps the conversion from Avro/Arrow schema to InternalRow */
public class InternalRowIterator implements Iterator<InternalRow> {
  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private final BigQueryStorageReadRowsTracer bigQueryStorageReadRowsTracer;
  private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
  private static final Logger log = LoggerFactory.getLogger(InternalRowIterator.class);

  public InternalRowIterator(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToInternalRowIteratorConverter converter,
      ReadRowsHelper readRowsHelper,
      BigQueryStorageReadRowsTracer bigQueryStorageReadRowsTracer) {
    this.readRowsResponses = readRowsResponses;
    this.converter = converter;
    this.readRowsHelper = readRowsHelper;
    this.bigQueryStorageReadRowsTracer = bigQueryStorageReadRowsTracer;
  }

  @Override
  public boolean hasNext() {
    while (!rows.hasNext()) {
      bigQueryStorageReadRowsTracer.readRowsResponseRequested();
      if (!readRowsResponses.hasNext()) {
        try {
          bigQueryStorageReadRowsTracer.finished();
        } catch (Exception e) {
          log.debug("Failure finishing tracer. stream:{} exception:{}", readRowsHelper, e);
        } finally {
          readRowsHelper.close();
        }
        return false;
      }
      ReadRowsResponse readRowsResponse = readRowsResponses.next();

      // TODO AQIU: this is not hit! this is not where we should decompress!
      // UnknownFieldSet unknownFieldSet = readRowsResponse.getUnknownFields();
      // java.util.Map<Integer, UnknownFieldSet.Field> unknownFieldSetMap = unknownFieldSet.asMap();
      // log.info(
      //     "AQIU: InternalRowIterator ReadRowsResponse UnknownFieldSet.asMap {}",
      //     unknownFieldSetMap);

      bigQueryStorageReadRowsTracer.readRowsResponseObtained(
          readRowsResponse == null ? 0 : converter.getBatchSizeInBytes(readRowsResponse));
      bigQueryStorageReadRowsTracer.nextBatchNeeded();
      bigQueryStorageReadRowsTracer.rowsParseStarted();
      rows = converter.convert(readRowsResponse);
    }

    return true;
  }

  @Override
  public InternalRow next() {
    return rows.next();
  }
}
