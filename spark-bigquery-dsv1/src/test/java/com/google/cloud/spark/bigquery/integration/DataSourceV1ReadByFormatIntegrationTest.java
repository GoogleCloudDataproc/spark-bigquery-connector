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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DataSourceV1ReadByFormatIntegrationTest extends ReadByFormatIntegrationTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> formats() {
    return Arrays.asList(new Object[][] {{"AVRO"}, {"ARROW"}});
  }

  public DataSourceV1ReadByFormatIntegrationTest(String format) {
    super(format);
  }

  @Test
  public void testOptimizedCountStarWithFilter() {
    DirectBigQueryRelation.emptyRowRDDsCreated = 0;
    long oldMethodCount =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("optimizedEmptyProjection", "false")
            .option("readDataFormat", dataFormat)
            .load()
            .select("corpus_date")
            .where("corpus_date > 0")
            .count();

    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated).isEqualTo(0);

    long optimizedCount =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", dataFormat)
            .load()
            .where("corpus_date > 0")
            .count();

    assertThat(optimizedCount).isEqualTo(oldMethodCount);
    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated).isEqualTo(1);
  }

  @Test
  public void testOptimizedCountStar() {
    DirectBigQueryRelation.emptyRowRDDsCreated = 0;
    long oldMethodCount =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("optimizedEmptyProjection", "false")
            .option("readDataFormat", dataFormat)
            .load()
            .select("corpus_date")
            .count();

    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated).isEqualTo(0);

    long optimizedCount =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .option("readDataFormat", dataFormat)
            .load()
            .count();

    assertThat(optimizedCount).isEqualTo(oldMethodCount);
    assertThat(DirectBigQueryRelation.emptyRowRDDsCreated).isEqualTo(1);
  }

  // additional tests are from the super-class

}
