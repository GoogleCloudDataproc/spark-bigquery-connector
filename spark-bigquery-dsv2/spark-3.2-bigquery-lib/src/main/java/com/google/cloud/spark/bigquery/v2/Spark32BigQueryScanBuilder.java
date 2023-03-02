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

import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Optional;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;

public class Spark32BigQueryScanBuilder extends Spark31BigQueryScanBuilder
    implements SupportsRuntimeFiltering {

  public Spark32BigQueryScanBuilder(BigQueryDataSourceReaderContext ctx) {
    super(ctx);
  }

  @Override
  public NamedReference[] filterAttributes() {
    Optional<String> partitionField = BigQueryUtil.getPartitionField(ctx.getTableInfo());
    ImmutableList<String> clusteringFields = BigQueryUtil.getClusteringFields(ctx.getTableInfo());

    ImmutableSet.Builder<String> filterFieldsBuilder = ImmutableSet.builder();
    partitionField.ifPresent(filterFieldsBuilder::add);
    filterFieldsBuilder.addAll(clusteringFields);
    ImmutableSet<String> filterFields = filterFieldsBuilder.build();
    return Arrays.stream(ctx.readSchema().fieldNames())
        .filter(filterFields::contains)
        .map(Expressions::column)
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(Filter[] filters) {
    ctx.filter(filters);
  }
}
