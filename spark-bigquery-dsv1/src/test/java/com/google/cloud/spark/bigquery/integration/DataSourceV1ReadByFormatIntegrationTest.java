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

import com.google.cloud.spark.bigquery.DataSourceVersion;
import java.util.Arrays;
import java.util.Collection;
import org.apache.spark.sql.types.DataTypes;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DataSourceV1ReadByFormatIntegrationTest extends ReadByFormatIntegrationTestBase {

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("AVRO", "ARROW");
  }

  public DataSourceV1ReadByFormatIntegrationTest(String dataFormat) {
    super(dataFormat, /* userProvidedSchemaAllowed */ false, DataTypes.TimestampNTZType);
    this.dataSourceVersion = DataSourceVersion.V1;
  }

  // additional tests are from the super-class

}
