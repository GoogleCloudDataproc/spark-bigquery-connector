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
