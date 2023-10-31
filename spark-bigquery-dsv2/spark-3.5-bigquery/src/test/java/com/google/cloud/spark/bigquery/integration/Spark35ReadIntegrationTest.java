/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import static com.google.cloud.spark.bigquery.integration.TestConstants.ALL_TYPES_TABLE_SCHEMA;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;

public class Spark35ReadIntegrationTest extends ReadIntegrationTestBase {
  private static StructType allTypesSchemaWithTimestampNTZ;

  @BeforeClass
  public static void intializeSchema() {
    allTypesSchemaWithTimestampNTZ = new StructType();
    for (StructField field : ALL_TYPES_TABLE_SCHEMA.fields()) {
      DataType dateTimeType =
          field.name().equals("dt") ? DataTypes.TimestampNTZType : field.dataType();
      allTypesSchemaWithTimestampNTZ =
          allTypesSchemaWithTimestampNTZ.add(
              field.name(), dateTimeType, field.nullable(), field.metadata());
    }
  }

  public Spark35ReadIntegrationTest() {

    super(/* userProvidedSchemaAllowed */ false, allTypesSchemaWithTimestampNTZ);
  }

  // tests are from the super-class
}
