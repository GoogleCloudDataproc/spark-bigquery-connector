package com.google.cloud.spark.bigquery.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericIntermediateDataCleaner extends Thread {
  private static final Logger logger =
      LoggerFactory.getLogger(GenericIntermediateDataCleaner.class);

  /** the path to delete */
  private final Path path;
  /** the hadoop configuration */
  private final Configuration conf;

  public GenericIntermediateDataCleaner(Path path, Configuration conf) {
    this.path = path;
    this.conf = conf;
  }

  @Override
  public void run() {
    deletePath();
  }

  void deletePath() {
    try {
      FileSystem fs = path.getFileSystem(conf);
      if (pathExists(fs, path)) {
        fs.delete(path, true);
      }
    } catch (Exception e) {
      logger.error("Failed to delete path " + path, e);
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
