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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class BigQueryTable implements Table, SupportsRead {

  private BigQueryDataSourceReaderContext ctx;

  public BigQueryTable(BigQueryDataSourceReaderContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new BigQueryScanBuilder(ctx);
  }

  @Override
  public String name() {
    return ctx.getTableName();
  }

  @Override
  public StructType schema() {
    return ctx.readSchema();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.<TableCapability>of(TableCapability.BATCH_READ);
  }
}
