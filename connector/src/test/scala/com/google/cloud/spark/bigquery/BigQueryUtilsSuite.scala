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
package com.google.cloud.spark.bigquery

import java.util.Optional

import com.google.cloud.bigquery.TableId
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}

class BigQueryUtilsSuite extends org.scalatest.FunSuite {

  val TABLE_ID = TableId.of("test.org:test-project", "test_dataset", "test_table")
  val FULLY_QUALIFIED_TABLE = "test.org:test-project.test_dataset.test_table"

  test("parse fully qualified table") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil
      .parseTableId(FULLY_QUALIFIED_TABLE)
    assert(tableId == TABLE_ID)
  }

  test("parse fully qualified legacy table") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil
      .parseTableId("test.org:test-project.test_dataset.test_table")
    assert(tableId == TABLE_ID)
  }

  test("parse invalid table") {
    assertThrows[IllegalArgumentException] {
      com.google.cloud.bigquery.connector.common.BigQueryUtil
        .parseTableId("test-org:test-project.table")
    }
  }

  test("parse fully qualified table with defaults") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId(
      FULLY_QUALIFIED_TABLE, Optional.of("other_dataset"), Optional.of("other-project"))
    assert(tableId == TABLE_ID)
  }

  test("parse partially qualified table") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil
      .parseTableId("test_dataset.test_table")
    assert(tableId == TableId.of("test_dataset", "test_table"))
  }

  test("parse partially qualified table with defaults") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId(
      "test_dataset.test_table", Optional.of("other_dataset"), Optional.of("default-project"))
    assert(tableId == TableId.of("default-project", "test_dataset", "test_table"))
  }

  test("parse unqualified table with defaults") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId(
      "test_table", Optional.of("default_dataset"), Optional.of("default-project"))
    assert(tableId == TableId.of("default-project", "default_dataset", "test_table"))
  }

  test("parse fully qualified partitioned table") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil
      .parseTableId(FULLY_QUALIFIED_TABLE + "$12345")
    assert(tableId == TableId.of("test.org:test-project", "test_dataset", "test_table$12345"))
  }

  test("parse unqualified partitioned table") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId(
      "test_table$12345", Optional.of("default_dataset"), Optional.empty())
    assert(tableId == TableId.of("default_dataset", "test_table$12345"))
  }

  test("parse table with date partition") {
    val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId(
      "test_table", Optional.of("default_dataset"), Optional.empty(), Optional.of("20200101"))
    assert(tableId == TableId.of("default_dataset", "test_table$20200101"))
  }

  test("unparsable table") {
    assertThrows[IllegalArgumentException] {
      val tableId = com.google.cloud.bigquery.connector.common.BigQueryUtil
        .parseTableId("foo:bar:baz")
    }
  }

  test("friendly name") {
    val name = BigQueryUtil.friendlyTableName(TABLE_ID)
    print(name + "\n")
    assert(name == FULLY_QUALIFIED_TABLE)
  }

  test("short friendly name") {
    val name = BigQueryUtil.friendlyTableName(TableId.of("test_dataset", "test_table"))
    assert(name == "test_dataset.test_table")
  }

  test("ToIteratorTest") {
    val path = new Path("connector/src/test/resources/ToIteratorTest")
    val fs = path.getFileSystem(new Configuration())
    var it = ToIterator(fs.listFiles(path, false))

    assert(it.isInstanceOf[scala.collection.Iterator[LocatedFileStatus]])
    assert(it.size == 2)

    // fresh instance
    it = ToIterator(fs.listFiles(path, false))
    assert(it.filter(f => f.getPath.getName.endsWith(".txt"))
      .next.getPath.getName.endsWith("file1.txt"))
  }

}
