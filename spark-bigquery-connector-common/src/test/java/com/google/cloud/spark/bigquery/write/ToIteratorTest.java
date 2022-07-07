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
package com.google.cloud.spark.bigquery.write;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class ToIteratorTest {

  static java.nio.file.Path testDir;

  @BeforeClass
  public static void createTestDirectory() throws Exception {
    testDir = Files.createTempDirectory("ToIteratorTest");
    testDir.toFile().deleteOnExit();
    Files.copy(
        ToIteratorTest.class.getResourceAsStream("/ToIteratorTest/file1.txt"),
        testDir.resolve("file1.txt"));
    Files.copy(
        ToIteratorTest.class.getResourceAsStream("/ToIteratorTest/file2.csv"),
        testDir.resolve("file2.csv"));
  }

  @Test
  public void toIteratorTest() throws Exception {
    Path path = new Path(testDir.toFile().getAbsolutePath());
    FileSystem fs = path.getFileSystem(new Configuration());
    ToIterator<LocatedFileStatus> it = new ToIterator<LocatedFileStatus>(fs.listFiles(path, false));

    assertThat(Iterators.size(it)).isEqualTo(2);

    // fresh instance
    it = new ToIterator<LocatedFileStatus>(fs.listFiles(path, false));
    List<LocatedFileStatus> textFiles =
        Streams.stream(it)
            .filter(f -> f.getPath().getName().endsWith(".txt"))
            .collect(Collectors.toList());
    assertThat(textFiles).hasSize(1);
    assertThat(textFiles.iterator().next().getPath().getName()).endsWith("file1.txt");
  }
}
