/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.acceptance;

public class AcceptanceTestConstants {

  public static final String MIN_BIG_NUMERIC =
      "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";

  public static final String MAX_BIG_NUMERIC =
      "578960446186580977117854925043439539266.34992332820282019728792003956564819967";

  public static final String BIGNUMERIC_TABLE_QUERY_TEMPLATE =
      "create table %s.%s (\n"
          + "    min bignumeric,\n"
          + "    max bignumeric\n"
          + "    ) \n"
          + "    as \n"
          + "    select \n"
          + "    cast(\""
          + MIN_BIG_NUMERIC
          + "\" as bignumeric) as min,\n"
          + "    cast(\""
          + MAX_BIG_NUMERIC
          + "\" as bignumeric) as max";
}
