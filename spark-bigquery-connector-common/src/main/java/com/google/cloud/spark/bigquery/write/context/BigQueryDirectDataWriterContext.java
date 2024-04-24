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
package com.google.cloud.spark.bigquery.write.context;

import static com.google.cloud.spark.bigquery.ProtobufUtils.ProtobufSchemaFieldCacheEntry;
import static com.google.cloud.spark.bigquery.ProtobufUtils.buildSingleRowMessage;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryDirectDataWriterHelper;
import com.google.cloud.bigquery.connector.common.WriteStreamStatistics;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectDataWriterContext implements DataWriterContext<InternalRow> {
  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataWriterContext.class);

  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final String tablePath;
  private final StructType sparkSchema;
  private final Descriptors.Descriptor schemaDescriptor;
  private final Map<Integer, ProtobufSchemaFieldCacheEntry> fieldIndexToEntryMap;
  private final DynamicMessage.Builder messageBuilder;

  /**
   * A helper object to assist the BigQueryDataWriter with all the writing: essentially does all the
   * interaction with BigQuery Storage Write API.
   */
  private BigQueryDirectDataWriterHelper writerHelper;

  public BigQueryDirectDataWriterContext(
      int partitionId,
      long taskId,
      long epochId,
      BigQueryClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoSchema protoSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings,
      Optional<String> traceId,
      boolean writeAtLeastOnce) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.tablePath = tablePath;
    this.sparkSchema = sparkSchema;
    try {
      this.schemaDescriptor = toDescriptor(sparkSchema);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new BigQueryConnectorException.InvalidSchemaException(
          "Could not convert spark-schema to descriptor object", e);
    }
    this.fieldIndexToEntryMap = new HashMap<>();
    this.messageBuilder = DynamicMessage.newBuilder(this.schemaDescriptor);

    this.writerHelper =
        new BigQueryDirectDataWriterHelper(
            writeClientFactory,
            tablePath,
            protoSchema,
            bigqueryDataWriterHelperRetrySettings,
            traceId,
            partitionId,
            writeAtLeastOnce);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    ByteString message =
        buildSingleRowMessage(
                sparkSchema,
                schemaDescriptor,
                record,
                java.util.Optional.of(fieldIndexToEntryMap),
                messageBuilder)
            .toByteString();
    writerHelper.addRow(message);
  }

  @Override
  public WriterCommitMessageContext commit() throws IOException {
    logger.debug("Data Writer {} finalizeStream()", partitionId);

    WriteStreamStatistics stats = writerHelper.finalizeStream();
    String writeStreamName = writerHelper.getWriteStreamName();

    logger.debug(
        "Data Writer {}'s write-stream has finalized with row count: {}", partitionId, stats.getRowCount());

    return new BigQueryDirectWriterCommitMessageContext(
        writeStreamName,
        partitionId,
        taskId,
        epochId,
        tablePath,
        stats.getRowCount(),
        stats.getBytesWritten());
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Data Writer {} abort()", partitionId);
    writerHelper.abort();
  }

  @Override
  public void close() throws IOException {
    // empty
  }
}
