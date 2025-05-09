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
package com.google.cloud.spark.bigquery;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import scala.Option;

public class NoSuchBigQueryTableException extends NoSuchTableException {

  public NoSuchBigQueryTableException(String db, String table) {
    super(db, table);
  }

  public NoSuchBigQueryTableException(Identifier tableIdent) {
    super(tableIdent);
  }

  public NoSuchBigQueryTableException(String message, Option<Throwable> cause) {
    super(message, cause);
  }
}
