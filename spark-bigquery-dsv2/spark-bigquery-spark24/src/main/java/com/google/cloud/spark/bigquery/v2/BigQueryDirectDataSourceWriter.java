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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.storage.v1beta2.BigQueryWriteClient;
import com.google.cloud.spark.bigquery.common.GenericBigQueryDirectDataSourceWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectDataSourceWriter implements DataSourceWriter {

  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataSourceWriter.class);
  private BigQueryWriteClient writeClient;
  private GenericBigQueryDirectDataSourceWriter dataSourceWriterHelper;

  public BigQueryDirectDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings)
      throws IllegalArgumentException {
    this.dataSourceWriterHelper =
        new GenericBigQueryDirectDataSourceWriter(
            bigQueryClient,
            bigQueryWriteClientFactory,
            destinationTableId,
            writeUUID,
            saveMode,
            sparkSchema,
            bigqueryDataWriterHelperRetrySettings);
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryDirectDataWriterFactory(
        this.dataSourceWriterHelper.getWriteClientFactory(),
        this.dataSourceWriterHelper.getTablePathForBigQueryStorage(),
        this.dataSourceWriterHelper.getSparkSchema(),
        this.dataSourceWriterHelper.getProtoSchema(),
        this.dataSourceWriterHelper.isIgnoreInputs(),
        this.dataSourceWriterHelper.getBigqueryDataWriterHelperRetrySettings());
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {}

  /**
   * This function will determine, based on the WritingMode: if in IGNORE_INPUTS mode, no work is to
   * be done; otherwise all streams will be batch committed using the BigQuery Storage Write API,
   * and then: if in OVERWRITE mode, the overwriteDestinationWithTemporary function from
   * BigQueryClient will be called to replace the destination table with all the data from the
   * temporary table; if in ALL_ELSE mode no more work needs to be done.
   *
   * <p>// * @see WritingMode
   *
   * @see BigQueryClient#overwriteDestinationWithTemporary(TableId temporaryTableId, TableId
   *     destinationTableId)
   * @param messages the BigQueryWriterCommitMessage array returned by the BigQueryDataWriter's.
   */
  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (this.dataSourceWriterHelper.evaluateIfIgnoreInput()) return;
    List<String> streamNames = new ArrayList<String>();
    for (WriterCommitMessage message : messages) {
      streamNames.add(((BigQueryDirectWriterCommitMessage) message).getWriteStreamName());
    }
    this.dataSourceWriterHelper.commit(streamNames, logger);
  }

  /**
   * If not in WritingMode IGNORE_INPUTS, the BigQuery Storage Write API WriteClient is shut down.
   *
   * @see BigQueryWriteClient
   * @param messages the BigQueryWriterCommitMessage array returned by the BigQueryDataWriter's.
   */
  @Override
  public void abort(WriterCommitMessage[] messages) {
    this.dataSourceWriterHelper.abort(logger);
  }
}
