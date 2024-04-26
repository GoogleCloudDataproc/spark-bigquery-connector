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
package com.google.cloud.spark.bigquery.util;

import static com.google.cloud.spark.bigquery.util.HdfsUtils.computeDirectorySizeInBytes;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class HdfsUtilsTest {

  static java.nio.file.Path testDir;

  @BeforeClass
  public static void createTestDirectory() throws Exception {
    testDir = Files.createTempDirectory("ToIteratorTest");
    testDir.toFile().deleteOnExit();
    Files.copy(
        HdfsUtilsTest.class.getResourceAsStream("/ToIteratorTest/file1.txt"),
        testDir.resolve("file1.txt"));
    Files.copy(
        HdfsUtilsTest.class.getResourceAsStream("/ToIteratorTest/file2.csv"),
        testDir.resolve("file2.csv"));
  }

  @Test
  public void toIteratorTest() throws Exception {
    Path path = new Path(testDir.toFile().getAbsolutePath());
    FileSystem fs = path.getFileSystem(new Configuration());
    Iterator<LocatedFileStatus> it = HdfsUtils.toJavaUtilIterator(fs.listFiles(path, false));

    assertThat(Iterators.size(it)).isEqualTo(2);

    // fresh instance
    it = HdfsUtils.toJavaUtilIterator(fs.listFiles(path, false));
    List<LocatedFileStatus> textFiles =
        Streams.stream(it)
            .filter(f -> f.getPath().getName().endsWith(".txt"))
            .collect(Collectors.toList());
    assertThat(textFiles).hasSize(1);
    assertThat(textFiles.iterator().next().getPath().getName()).endsWith("file1.txt");
  }

  @Test
  public void testComputeDirectorySizeInBytes() throws IOException {
    Path path =
        new Path(Files.createTempDirectory("ComputeDirectorySizeTest").toFile().getAbsolutePath());
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);

    Path file1 = new Path(path, "file1.txt");
    String content1 = "File 1 ....!!!";
    try (FSDataOutputStream out = fs.create(file1)) {
      out.write(content1.getBytes());
      out.flush();
    }

    Path file2 = new Path(path, "file2.txt");
    String content2 = "File 2 in the same directory.";
    try (FSDataOutputStream out = fs.create(file2)) {
      out.write(content2.getBytes());
      out.flush();
    }

    Path subDir = new Path(path, "subdir");
    fs.mkdirs(subDir);

    Path file3 = new Path(subDir, "file3.txt");
    String content3 = "File 3 in subdir.";
    try (FSDataOutputStream out = fs.create(file3)) {
      out.write(content3.getBytes());
      out.flush();
    }

    Path successFile = new Path(subDir, "_SUCCESS");
    String content4 = "_SUCCESS File to be ignored.";
    try (FSDataOutputStream out = fs.create(successFile)) {
      out.write(content4.getBytes());
      out.flush();
    }

    long expectedSize =
        fs.getFileStatus(file1).getLen()
            + fs.getFileStatus(file2).getLen()
            + fs.getFileStatus(file3).getLen();

    long actualSize = computeDirectorySizeInBytes(path, conf);

    fs.delete(path, true);

    assertThat(actualSize).isEqualTo(expectedSize);
  }
}
