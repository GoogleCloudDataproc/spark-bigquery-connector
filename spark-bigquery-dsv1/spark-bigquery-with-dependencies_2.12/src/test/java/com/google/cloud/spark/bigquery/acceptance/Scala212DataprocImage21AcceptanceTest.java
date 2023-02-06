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
package com.google.cloud.spark.bigquery.acceptance;

import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class Scala212DataprocImage21AcceptanceTest extends DataprocAcceptanceTestBase {

  private static AcceptanceTestContext context;

  public Scala212DataprocImage21AcceptanceTest() {
    super(context);
  }

  @BeforeClass
  public static void setup() throws Exception {
    context =
        DataprocAcceptanceTestBase.setup("2.1-debian11", "spark-bigquery", Collections.emptyList());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    DataprocAcceptanceTestBase.tearDown(context);
  }
}
