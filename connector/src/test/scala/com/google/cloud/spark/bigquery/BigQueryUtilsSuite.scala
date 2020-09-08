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

class BigQueryUtilScalaSuite extends org.scalatest.FunSuite {

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
