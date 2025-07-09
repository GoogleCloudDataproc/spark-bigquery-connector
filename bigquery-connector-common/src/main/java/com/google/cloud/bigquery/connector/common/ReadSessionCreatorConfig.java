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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class ReadSessionCreatorConfig {
  private final boolean viewsEnabled;
  private final String viewEnabledParamName;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;
  private final int materializationExpirationTimeInMinutes;
  private final DataFormat readDataFormat;
  private final int maxReadRowsRetries;
  private final OptionalInt maxParallelism;
  private final OptionalInt preferredMinParallelism;
  private final int defaultParallelism;
  private final Optional<String> requestEncodedBase;
  private final Optional<String> bigQueryStorageGrpcEndpoint;
  private final Optional<String> bigQueryHttpEndpoint;
  private final int backgroundParsingThreads;
  private final boolean pushAllFilters;
  private final int prebufferResponses;
  private final int streamsPerPartition;
  private final CompressionCodec arrowCompressionCodec;
  private final ResponseCompressionCodec responseCompressionCodec;
  private final Optional<String> traceId;
  private final boolean enableReadSessionCaching;
  private final long readSessionCacheDurationMins;
  private final OptionalLong snapshotTimeMillis;

  ReadSessionCreatorConfig(
      boolean viewsEnabled,
      Optional<String> materializationProject,
      Optional<String> materializationDataset,
      int materializationExpirationTimeInMinutes,
      DataFormat readDataFormat,
      int maxReadRowsRetries,
      String viewEnabledParamName,
      OptionalInt maxParallelism,
      OptionalInt preferredMinParallelism,
      int defaultParallelism,
      Optional<String> requestEncodedBase,
      Optional<String> bigQueryStorageGrpcEndpoint,
      Optional<String> bigQueryHttpEndpoint,
      int backgroundParsingThreads,
      boolean pushAllFilters,
      int prebufferResponses,
      int streamsPerPartition,
      CompressionCodec arrowCompressionCodec,
      ResponseCompressionCodec responseCompressionCodec,
      Optional<String> traceId,
      boolean enableReadSessionCaching,
      long readSessionCacheDurationMins,
      OptionalLong snapshotTimeMillis) {
    this.viewsEnabled = viewsEnabled;
    this.viewEnabledParamName = viewEnabledParamName;
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
    this.materializationExpirationTimeInMinutes = materializationExpirationTimeInMinutes;
    this.readDataFormat = readDataFormat;
    this.maxReadRowsRetries = maxReadRowsRetries;
    this.maxParallelism = maxParallelism;
    this.preferredMinParallelism = preferredMinParallelism;
    this.defaultParallelism = defaultParallelism;
    this.requestEncodedBase = requestEncodedBase;
    this.bigQueryStorageGrpcEndpoint = bigQueryStorageGrpcEndpoint;
    this.bigQueryHttpEndpoint = bigQueryHttpEndpoint;
    this.backgroundParsingThreads = backgroundParsingThreads;
    this.pushAllFilters = pushAllFilters;
    this.prebufferResponses = prebufferResponses;
    this.streamsPerPartition = streamsPerPartition;
    this.arrowCompressionCodec = arrowCompressionCodec;
    this.responseCompressionCodec = responseCompressionCodec;
    this.traceId = traceId;
    this.enableReadSessionCaching = enableReadSessionCaching;
    this.readSessionCacheDurationMins = readSessionCacheDurationMins;
    this.snapshotTimeMillis = snapshotTimeMillis;
  }

  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  public String getViewEnabledParamName() {
    return viewEnabledParamName;
  }

  public Optional<String> getMaterializationProject() {
    return materializationProject;
  }

  public Optional<String> getMaterializationDataset() {
    return materializationDataset;
  }

  public int getMaterializationExpirationTimeInMinutes() {
    return materializationExpirationTimeInMinutes;
  }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  public CompressionCodec getArrowCompressionCodec() {
    return arrowCompressionCodec;
  }

  public ResponseCompressionCodec getResponseCompressionCodec() {
    return responseCompressionCodec;
  }

  public int getMaxReadRowsRetries() {
    return maxReadRowsRetries;
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism;
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Optional<String> getRequestEncodedBase() {
    return this.requestEncodedBase;
  }

  public Optional<String> getBigQueryStorageGrpcEndpoint() {
    return bigQueryStorageGrpcEndpoint;
  }

  public Optional<String> getBigQueryHttpEndpoint() {
    return bigQueryHttpEndpoint;
  }

  public int backgroundParsingThreads() {
    return this.backgroundParsingThreads;
  }

  public boolean getPushAllFilters() {
    return this.pushAllFilters;
  }

  public ReadRowsHelper.Options toReadRowsHelperOptions() {
    return new ReadRowsHelper.Options(
        getMaxReadRowsRetries(),
        getBigQueryStorageGrpcEndpoint(),
        backgroundParsingThreads(),
        getPrebufferResponses());
  }

  public int streamsPerPartition() {
    return streamsPerPartition;
  }

  public int getPrebufferResponses() {
    return prebufferResponses;
  }

  public Optional<String> getTraceId() {
    return traceId;
  }

  public OptionalInt getPreferredMinParallelism() {
    return preferredMinParallelism;
  }

  public boolean isReadSessionCachingEnabled() {
    return enableReadSessionCaching;
  }

  public long getReadSessionCacheDurationMins() {
    return readSessionCacheDurationMins;
  }

  public OptionalLong getSnapshotTimeMillis() {
    return snapshotTimeMillis;
  }
}
