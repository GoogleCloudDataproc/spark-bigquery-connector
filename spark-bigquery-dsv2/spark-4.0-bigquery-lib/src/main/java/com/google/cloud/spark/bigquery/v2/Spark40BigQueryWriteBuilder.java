/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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

import com.google.cloud.spark.bigquery.write.CreatableRelationProviderHelper;
import com.google.inject.Injector;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.sources.InsertableRelation;

public class Spark40BigQueryWriteBuilder extends Spark35BigQueryWriteBuilder implements Write, V1Write {

  public Spark40BigQueryWriteBuilder(Injector injector, LogicalWriteInfo info, SaveMode mode) {
    super(injector, info, mode);
  }
}
