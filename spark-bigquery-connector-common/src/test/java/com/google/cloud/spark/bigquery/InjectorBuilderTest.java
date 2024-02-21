/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class InjectorBuilderTest {

  @Test
  public void testDefaults() {
    SparkSession unused = SparkSession.builder().master("local[1]").getOrCreate();
    Injector injector =
        new InjectorBuilder().withOptions(ImmutableMap.of("table", "foo.bar")).build();
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    assertThat(config.getTableId().getTable()).isEqualTo("bar");
    UserAgentProvider userAgentProvider = injector.getInstance(UserAgentProvider.class);
    assertThat(userAgentProvider.getUserAgent()).contains("v2");
  }

  @Test
  public void testParams() {
    SparkSession spark = SparkSession.builder().master("local[1]").appName("test").getOrCreate();
    ImmutableMap<String, String> options =
        ImmutableMap.<String, String>builder()
            .put("table", "foo.bar")
            .put("GPN", "testUser")
            .build();
    Injector injector =
        new InjectorBuilder()
            .withOptions(options)
            .withCustomDefaults(ImmutableMap.of("writeMethod", "INDIRECT"))
            .withSpark(spark)
            .withDataSourceVersion(DataSourceVersion.V1)
            .withSchema(new StructType())
            .withTableIsMandatory(false)
            .build();
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    assertThat(config.getTableId().getTable()).isEqualTo("bar");
    assertThat(config.getWriteMethod()).isEqualTo(SparkBigQueryConfig.WriteMethod.INDIRECT);
    UserAgentProvider userAgentProvider = injector.getInstance(UserAgentProvider.class);
    assertThat(userAgentProvider.getUserAgent()).contains("v1");
    assertThat(userAgentProvider.getUserAgent()).contains("testUser");
  }

  @Test
  public void testConnectorInfo() {
    SparkSession.builder()
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate();
    ImmutableMap<String, String> options =
        ImmutableMap.<String, String>builder()
            .put("table", "foo.bar")
            .put("GPN", "testUser")
            .build();
    Injector injector = new InjectorBuilder().withOptions(options).build();
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    assertThat(config.getTableId().getTable()).isEqualTo("bar");
    UserAgentProvider userAgentProvider = injector.getInstance(UserAgentProvider.class);
    assertThat(userAgentProvider.getConnectorInfo()).contains("v2");
    assertThat(userAgentProvider.getConnectorInfo()).contains("testUser");
  }
}
