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
package com.google.cloud.spark.bigquery.v2.context;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class BigQueryDataSourceReaderContextTest {

  @Test
  public void testEmptyProjectionPartitionsLargerThanIntMax() {
    long rowCount = 13_927_954_135L;
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    when(bigQueryClient.calculateTableSize(any(TableId.class), any())).thenReturn(rowCount);
    TableInfo tableInfo =
        TableInfo.of(
            TableId.of("project", "dataset", "table"),
            StandardTableDefinition.of(Schema.of(Field.of("name", StandardSQLTypeName.STRING))));

    BigQueryDataSourceReaderContext ctx =
        new BigQueryDataSourceReaderContext(
            tableInfo,
            bigQueryClient,
            /* bigQueryReadClientFactory= */ null,
            /* tracerFactory= */ null,
            new ReadSessionCreatorConfigBuilder().setDefaultParallelism(2).build(),
            /* globalFilter= */ Optional.empty(),
            /* schema= */ Optional.of(new StructType()),
            "applicationId",
            mock(SparkBigQueryConfig.class),
            /* sqlContext= */ null,
            /* sparkSession= */ null,
            /* readTableOptions= */ null);

    List<Long> partitionSizes =
        ctx.planInputPartitionContexts()
            .map(partition -> ((EmptyProjectionInputPartitionContext) partition).partitionSize)
            .collect(Collectors.toList());

    assertThat(partitionSizes).containsExactly(6_963_977_068L, 6_963_977_067L).inOrder();
    assertThat(partitionSizes.stream().mapToLong(Long::longValue).sum()).isEqualTo(rowCount);
  }
}
