package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.common.GenericIntermediateDataCleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Responsible for recursively deleting the intermediate path. Implementing Runnable in order to act
 * as shutdown hook.
 */
class IntermediateDataCleaner extends GenericIntermediateDataCleaner {

    IntermediateDataCleaner(Path path, Configuration conf) {
        super(path, conf);
    }

    @Override
    public void run() {
        super.run();
    }
}
