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
package com.google.cloud.spark.bigquery.acceptance;

import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.storage.*;
import com.google.common.io.ByteStreams;
import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class AcceptanceTestUtils {

  // must be set in order to run the acceptance test
  static final String BUCKET = System.getenv("ACCEPTANCE_TEST_BUCKET");
  private static final BigQuery bq = BigQueryOptions.getDefaultInstance().getService();

  static Storage storage =
      new StorageOptions.DefaultStorageFactory().create(StorageOptions.getDefaultInstance());

  public static Path getArtifact(Path targetDir, String prefix, String suffix) {
    Predicate<Path> prefixSuffixChecker = prefixSuffixChecker(prefix, suffix);
    try {
      return Files.list(targetDir)
          .filter(Files::isRegularFile)
          .filter(prefixSuffixChecker)
          .max(Comparator.comparing(AcceptanceTestUtils::lastModifiedTime))
          .get();
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
  }

  private static Predicate<Path> prefixSuffixChecker(final String prefix, final String suffix) {
    return path -> {
      String name = path.toFile().getName();
      return name.startsWith(prefix) && name.endsWith(suffix) && name.indexOf("-javadoc") == -1;
    };
  }

  private static FileTime lastModifiedTime(Path path) {
    try {
      return Files.getLastModifiedTime(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
  }

  public static BlobId copyToGcs(Path source, String destinationUri, String contentType)
      throws Exception {
    File sourceFile = source.toFile();
    try (FileInputStream sourceInputStream = new FileInputStream(sourceFile)) {
      FileChannel sourceFileChannel = sourceInputStream.getChannel();
      MappedByteBuffer sourceContent =
          sourceFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, sourceFile.length());
      return uploadToGcs(sourceContent, destinationUri, contentType);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to write '%s' to '%s'", source, destinationUri), e);
    }
  }

  public static BlobId uploadToGcs(InputStream source, String destinationUri, String contentType)
      throws Exception {
    try {
      ByteBuffer sourceContent = ByteBuffer.wrap(ByteStreams.toByteArray(source));
      return uploadToGcs(sourceContent, destinationUri, contentType);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write to '%s'", destinationUri), e);
    }
  }

  public static BlobId uploadToGcs(ByteBuffer content, String destinationUri, String contentType)
      throws Exception {
    URI uri = new URI(destinationUri);
    BlobId blobId = BlobId.of(uri.getAuthority(), uri.getPath().substring(1));
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();
    try (WriteChannel writer = storage.writer(blobInfo)) {
      writer.write(content);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write to '%s'", destinationUri), e);
    }
    return blobId;
  }

  public static String createTestBaseGcsDir(String testId) {
    return String.format("gs://%s/tests/%s", BUCKET, testId);
  }

  public static String getCsv(String resultsDirUri) throws Exception {
    URI uri = new URI(resultsDirUri);
    Blob csvBlob =
        StreamSupport.stream(
                storage
                    .list(
                        uri.getAuthority(),
                        Storage.BlobListOption.prefix(uri.getPath().substring(1)))
                    .iterateAll()
                    .spliterator(),
                false)
            .filter(blob -> blob.getName().endsWith("csv"))
            .findFirst()
            .get();
    return new String(storage.readAllBytes(csvBlob.getBlobId()), StandardCharsets.UTF_8);
  }

  public static void deleteGcsDir(String testBaseGcsDir) throws Exception {
    URI uri = new URI(testBaseGcsDir);
    BlobId[] blobIds =
        StreamSupport.stream(
                storage
                    .list(
                        uri.getAuthority(),
                        Storage.BlobListOption.prefix(uri.getPath().substring(1)))
                    .iterateAll()
                    .spliterator(),
                false)
            .map(Blob::getBlobId)
            .toArray(BlobId[]::new);
    if (blobIds.length > 1) {
      storage.delete(blobIds);
    }
  }

  public static void createBqDataset(String dataset) {
    DatasetId datasetId = DatasetId.of(dataset);
    bq.create(DatasetInfo.of(datasetId));
  }

  public static int getNumOfRowsOfBqTable(String dataset, String table) {
    return bq.getTable(dataset, table).getNumRows().intValue();
  }

  public static void runBqQuery(String query) throws Exception {
    bq.query(QueryJobConfiguration.of(query));
  }

  public static void deleteBqDatasetAndTables(String dataset) {
    bq.delete(DatasetId.of(dataset), DatasetDeleteOption.deleteContents());
  }

  static void uploadConnectorJar(String targetDir, String prefix, String connectorJarUri)
      throws Exception {
    Path targetDirPath = Paths.get(targetDir);
    Path assemblyJar = AcceptanceTestUtils.getArtifact(targetDirPath, prefix, ".jar");
    AcceptanceTestUtils.copyToGcs(assemblyJar, connectorJarUri, "application/java-archive");
  }

  public static String generateClusterName(String testId) {
    return String.format("sbc-acceptance-%s", testId);
  }
}
