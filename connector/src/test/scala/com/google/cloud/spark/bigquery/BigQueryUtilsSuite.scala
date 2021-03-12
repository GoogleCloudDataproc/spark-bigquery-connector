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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.Iterator

class BigQueryUtilScalaSuite extends AnyFunSuite with Matchers with OptionValues {

  test("ToIteratorTest") {
    val resourceFolder = this.getClass.getClassLoader.getResource("ToIteratorTest")

    val path = new Path(resourceFolder.toString)
    val fs = path.getFileSystem(new Configuration())

    val mkIterator = () => ToIterator(fs.listFiles(path, false))

    val it = mkIterator()

    it shouldBe an [Iterator[_]]
    it.size should be (2)

    // fresh iterator instance. The previous one is consumed already.
    val txtEntry = mkIterator().find(f => f.getPath.getName.endsWith(".txt"))
    txtEntry.value.getPath.getName should endWith("file1.txt")
  }

}
