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
package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Before;

public class Spark35IndirectWriteIntegrationTest extends WriteIntegrationTestBase {

  public Spark35IndirectWriteIntegrationTest() {
    super(SparkBigQueryConfig.WriteMethod.INDIRECT, DataTypes.TimestampNTZType);
  }

  @Before
  public void setParquetLoadBehaviour() {
    // TODO: make this the default value
    spark.conf().set("enableListInference", "true");
  }

  // tests from superclass
}
