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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.common.GenericBigQueryInputPartition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

// This class will be used at spark executor side to create inputPartition object.
public class BigQueryInputPartition extends GenericBigQueryInputPartition
    implements InputPartition<InternalRow> {

  public BigQueryInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
      String streamName,
      ReadRowsHelper.Options options,
      ReadRowsResponseToInternalRowIteratorConverter converter) {
    super(bigQueryReadClientFactory, streamName, options, converter);
  }

  // This method will Create the actual data reader and Read the data for corresponding RDD
  // partition.
  // It will return object of Bigquery Input Partition reader class
  @Override
  public InputPartitionReader<InternalRow> createPartitionReader() {
    return new BigQueryInputPartitionReader(
        super.getReadRowsResponse(), super.getConverter(), super.getReadRowsHelper());
  }
}
