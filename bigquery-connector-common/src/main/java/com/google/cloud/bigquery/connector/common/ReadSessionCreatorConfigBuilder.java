/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.util.Optional;
import java.util.OptionalInt;

public class ReadSessionCreatorConfigBuilder {

  private boolean viewsEnabled;
  private Optional<String> materializationProject = Optional.empty();
  private Optional<String> materializationDataset = Optional.empty();
  private int materializationExpirationTimeInMinutes = 120;
  private DataFormat readDataFormat = DataFormat.ARROW;
  private int maxReadRowsRetries = 10;
  private String viewEnabledParamName = "";
  private OptionalInt maxParallelism = OptionalInt.empty();
  private OptionalInt preferredMinParallelism = OptionalInt.empty();
  private int defaultParallelism = 1000;
  private Optional<String> requestEncodedBase = Optional.empty();
  private Optional<String> bigQueryStorageGrpcEndpoint = Optional.empty();
  private Optional<String> bigQueryHttpEndpoint = Optional.empty();
  private int backgroundParsingThreads = 0;
  private boolean pushAllFilters = true;
  int prebufferResponses = 1;
  int streamsPerPartition = 1;
  private CompressionCodec arrowCompressionCodec = CompressionCodec.COMPRESSION_UNSPECIFIED;
  private Optional<String> traceId = Optional.empty();

  public ReadSessionCreatorConfigBuilder setViewsEnabled(boolean viewsEnabled) {
    this.viewsEnabled = viewsEnabled;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setMaterializationProject(
      Optional<String> materializationProject) {
    this.materializationProject = materializationProject;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setMaterializationDataset(
      Optional<String> materializationDataset) {
    this.materializationDataset = materializationDataset;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setMaterializationExpirationTimeInMinutes(
      int materializationExpirationTimeInMinutes) {
    this.materializationExpirationTimeInMinutes = materializationExpirationTimeInMinutes;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setReadDataFormat(DataFormat readDataFormat) {
    this.readDataFormat = readDataFormat;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setMaxReadRowsRetries(int maxReadRowsRetries) {
    this.maxReadRowsRetries = maxReadRowsRetries;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setViewEnabledParamName(String viewEnabledParamName) {
    this.viewEnabledParamName = viewEnabledParamName;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setMaxParallelism(OptionalInt maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setPreferredMinParallelism(
      OptionalInt preferredMinParallelism) {
    this.preferredMinParallelism = preferredMinParallelism;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setDefaultParallelism(int defaultParallelism) {
    this.defaultParallelism = defaultParallelism;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setRequestEncodedBase(
      Optional<String> requestEncodedBase) {
    this.requestEncodedBase = requestEncodedBase;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setBigQueryStorageGrpcEndpoint(
      Optional<String> bigQueryStorageGrpcEndpoint) {
    this.bigQueryStorageGrpcEndpoint = bigQueryStorageGrpcEndpoint;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setBigQueryHttpEndpoint(
      Optional<String> bigQueryHttpEndpoint) {
    this.bigQueryHttpEndpoint = bigQueryHttpEndpoint;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setBackgroundParsingThreads(int backgroundParsingThreads) {
    this.backgroundParsingThreads = backgroundParsingThreads;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setPushAllFilters(boolean pushAllFilters) {
    this.pushAllFilters = pushAllFilters;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setPrebufferReadRowsResponses(int prebufferResponses) {
    this.prebufferResponses = prebufferResponses;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setStreamsPerPartition(int streamsPerPartition) {
    this.streamsPerPartition = streamsPerPartition;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setArrowCompressionCodec(
      CompressionCodec arrowCompressionCodec) {
    this.arrowCompressionCodec = arrowCompressionCodec;
    return this;
  }

  public ReadSessionCreatorConfigBuilder setTraceId(Optional<String> traceId) {
    this.traceId = traceId;
    return this;
  }

  public ReadSessionCreatorConfig build() {
    return new ReadSessionCreatorConfig(
        viewsEnabled,
        materializationProject,
        materializationDataset,
        materializationExpirationTimeInMinutes,
        readDataFormat,
        maxReadRowsRetries,
        viewEnabledParamName,
        maxParallelism,
        preferredMinParallelism,
        defaultParallelism,
        requestEncodedBase,
        bigQueryStorageGrpcEndpoint,
        bigQueryHttpEndpoint,
        backgroundParsingThreads,
        pushAllFilters,
        prebufferResponses,
        streamsPerPartition,
        arrowCompressionCodec,
        traceId);
  }
}
