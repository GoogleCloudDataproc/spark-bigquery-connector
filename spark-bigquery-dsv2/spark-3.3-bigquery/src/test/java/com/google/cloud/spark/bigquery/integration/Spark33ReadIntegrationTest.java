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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class Spark33ReadIntegrationTest extends ReadIntegrationTestBase {
    public Spark33ReadIntegrationTest() {
        super(/* userProvidedSchemaAllowed */ false);
    }

    // tests are from the super-class

    @Test
    public void testTpcDs() throws Exception {
        String[] tables = {"call_center",
                "catalog_page",
                "catalog_returns",
                "catalog_sales",
                "customer",
                "customer_address",
                "customer_demographics",
                "date_dim",
                "dbgen_version",
                "household_demographics",
                "income_band",
                "inventory",
                "item",
                "promotion",
                "reason",
                "ship_mode",
                "store",
                "store_returns",
                "store_sales",
                "time_dim",
                "warehouse",
                "web_page",
                "web_returns",
                "web_sales",
                "web_site"};
      for(String table:tables) {
        Dataset<Row> df = spark.read().format("bigquery").load("tpcds_1G." + table);
        df.createTempView(table);
      }

      String q55 = "select i_brand_id brand_id, i_brand brand,\n" +
              "   sum(ss_ext_sales_price) ext_price\n" +
              " from date_dim, store_sales, item\n" +
              " where d_date_sk = ss_sold_date_sk\n" +
              "   and ss_item_sk = i_item_sk\n" +
              "   and i_manager_id=28\n" +
              "   and d_moy=11\n" +
              "   and d_year=1999\n" +
              " group by i_brand, i_brand_id\n" +
              " order by ext_price desc, brand_id\n" +
              " limit 100";

      Dataset<Row> result = spark.sql(q55);
      assertThat(result.count()).isEqualTo(100);
      result.show();
    }

}
