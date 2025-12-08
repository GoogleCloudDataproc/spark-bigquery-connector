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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for recursively deleting the intermediate path. Implementing Runnable in order to act
 * as shutdown hook.
 */
public class IntermediateDataCleaner extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(IntermediateDataCleaner.class);

  /** the path to delete */
  private final Path path;
  /** the hadoop configuration */
  private final Configuration conf;
  /** the path for the job */
  private final Path gcsPathPrefix;

  public IntermediateDataCleaner(Path path, Configuration conf, Path gcsPathPrefix) {
    this.path = path;
    this.conf = conf;
    this.gcsPathPrefix = gcsPathPrefix;
  }

  @Override
  public void run() {
    deleteGcsPath();
  }

  public void deletePath() {
    try {
      logger.info("Deleting path " + path + " if it exists");
      FileSystem fs = path.getFileSystem(conf);
      if (pathExists(fs, path)) {
        fs.delete(path, true);
      }
      logger.info("Path " + path + " no longer exists)");
    } catch (Exception e) {
      logger.error("Failed to delete path " + path, e);
    }
  }

  // Delete all GCS path matched with the application Id.
  public void deleteGcsPath() {
    logger.info("Deleting Gcs path " + gcsPathPrefix + " if it exists");
    try {
      FileSystem fs = FileSystem.get(gcsPathPrefix.toUri(), conf);
      FileStatus[] statuses = fs.globStatus(gcsPathPrefix);

      if (statuses == null || statuses.length == 0) {
        logger.info("No paths found matching pattern: {}", gcsPathPrefix);
        return;
      }

      logger.info(
          "Found {} paths matching the pattern. Starting recursive deletion.", statuses.length);

      boolean allSuccess = true;
      for (FileStatus status : statuses) {
        Path pathToDelete = status.getPath();
        FileSystem deleteFs = FileSystem.get(pathToDelete.toUri(), conf);
        boolean deleted = deleteFs.delete(pathToDelete, true);

        if (deleted) {
          logger.info("Successfully deleted path: {}", pathToDelete);
        } else {
          logger.error("Failed to delete path: {}", pathToDelete);
          allSuccess = false;
        }
      }

      if (allSuccess) {
        logger.info("Completed cleanup for pattern: {}", gcsPathPrefix);
      } else {
        logger.warn(
            "Completed cleanup, but one or more paths failed to delete for pattern: {}",
            gcsPathPrefix);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteEpochPath(long epochId) {
    Path epochPath = new Path(path, String.valueOf(epochId));
    try {
      logger.info("Deleting epoch path " + epochPath + " if it exists");
      FileSystem fs = epochPath.getFileSystem(conf);
      if (pathExists(fs, epochPath)) {
        fs.delete(epochPath, true);
      }
      logger.info("Path " + epochPath + " no longer exists)");
    } catch (Exception e) {
      logger.error("Failed to delete path " + epochPath, e);
    }
  }

  // fs.exists can throw exception on missing path
  private boolean pathExists(FileSystem fs, Path path) {
    try {
      return fs.exists(path);
    } catch (Exception e) {
      return false;
    }
  }
}
