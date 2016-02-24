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

import java.io.File

import sbt.IO
import sbtassembly.MergeStrategy

/**
 * MergeStratagy that deduplicates and then performs a customer transformation on file basename.
 *
 * Used to rename custom META-INF resources JarJar does not affect.
 */
case class RelocationMergeStrategy(relocate: String => String) extends MergeStrategy {
  val name = s"relocation"

  def apply(tempDir: File, path: String, files: Seq[File]) =
  // First deduplicate to guarantee uniqueness.
    MergeStrategy.deduplicate.apply(tempDir, path, files) match {
      case Right(mappings) => Right(mappings.map(Function.tupled((f, p) => f -> relocate(p))))
      case error => error
    }
}

/**
 * MergeStrategy that transforms the paths services files and their contents.
 *
 * Used in place of Maven Shade's ServicesResourceTransformer.
 *
 * @param toRelocate       The prefixes to name
 * @param relocationPrefix The prefix to relocate under
 */
case class ServiceResourceMergeStrategy(
  toRelocate: Seq[String], relocationPrefix: String) extends MergeStrategy {
  val name = s"serviceResource"
  private val sysFileSep = System.getProperty("file.separator")

  def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
    // First filter distinct lines like normal services.
    val relocated = MergeStrategy.filterDistinctLines.apply(tempDir, path, files) match {
      case Right(mappings) => mappings.map {
        case (f: File, p: String) => relocateFile(tempDir, f, p)
      }
      case error => return error
    }
    Right(relocated)
  }

  private def relocateFile(tempDir: File, oldFile: File, oldPath: String): (File, String) = {
    val (dir, name) = oldPath.splitAt(oldPath.lastIndexOf(sysFileSep) + 1)
    // If the file does not need to be relocated ignore it.
    val path = dir + relocate(name).getOrElse(return oldFile -> oldPath)
    val file = MergeStrategy.createMergeTarget(tempDir, path)
    val newLines = IO.readLines(oldFile, IO.utf8)
      // Leave lines that do not need to be relocated alone
      .map(l => relocate(l).getOrElse(l))
    IO.writeLines(file, newLines, IO.utf8)
    file -> path
  }

  private def relocate(file: String): Option[String] = {
    toRelocate.find(file.startsWith).map(_ => s"$relocationPrefix.$file")
  }
}

