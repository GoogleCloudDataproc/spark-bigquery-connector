/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.inject.Injector;
import java.util.function.Supplier;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class Spark32BigQueryTable extends Spark31BigQueryTable {

  protected Spark32BigQueryTable(Injector injector, Supplier<StructType> schemaSupplier) {
    super(injector, schemaSupplier);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    BigQueryDataSourceReaderContext ctx = createBigQueryDataSourceReaderContext(options);
    return new Spark32BigQueryScanBuilder(ctx);
  }
}
