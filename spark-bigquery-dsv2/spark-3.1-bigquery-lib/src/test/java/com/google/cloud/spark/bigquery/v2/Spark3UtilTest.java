/*
 * Copyright 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Injector;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

public class Spark3UtilTest {

  private static final CaseInsensitiveStringMap EMPTY_OPTIONS =
      new CaseInsensitiveStringMap(Collections.emptyMap());

  @Test
  public void testGetSchemaOrThrow_returnsSparkProvidedSchema() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    SparkBigQueryConfig config = mock(SparkBigQueryConfig.class);
    StructType providedSchema = new StructType().add("id", DataTypes.LongType);

    StructType result = Spark3Util.getSchemaOrThrow(bigQueryClient, config, providedSchema);

    assertThat(result).isEqualTo(providedSchema);
    // The Spark provided schema short circuits the BigQuery metadata call.
    verify(bigQueryClient, never()).getReadTableSchema(any());
  }

  @Test
  public void testGetSchemaOrThrow_convertsBigQuerySchema() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    SparkBigQueryConfig config = mock(SparkBigQueryConfig.class);
    when(bigQueryClient.getReadTableSchema(any()))
        .thenReturn(
            Schema.of(
                Field.of("id", LegacySQLTypeName.INTEGER),
                Field.of("name", LegacySQLTypeName.STRING)));

    StructType result = Spark3Util.getSchemaOrThrow(bigQueryClient, config, null);

    assertThat(result.fieldNames()).asList().containsExactly("id", "name").inOrder();
    assertThat(result.apply("id").dataType()).isEqualTo(DataTypes.LongType);
    assertThat(result.apply("name").dataType()).isEqualTo(DataTypes.StringType);
  }

  @Test
  public void testGetSchemaOrThrow_throwsTableNotFoundExceptionWithProject() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    SparkBigQueryConfig config = mock(SparkBigQueryConfig.class);
    when(config.getTableId())
        .thenReturn(TableId.of("bigquery-public-data", "thelook1_ecommerce", "orders"));
    when(bigQueryClient.getReadTableSchema(any())).thenReturn(null);

    TableNotFoundException exception =
        assertThrows(
            TableNotFoundException.class,
            () -> Spark3Util.getSchemaOrThrow(bigQueryClient, config, null));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("Table bigquery-public-data.thelook1_ecommerce.orders not found");
  }

  @Test
  public void testGetSchemaOrThrow_throwsTableNotFoundExceptionWithoutProject() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    SparkBigQueryConfig config = mock(SparkBigQueryConfig.class);
    when(config.getTableId()).thenReturn(TableId.of("thelook1_ecommerce", "orders"));
    when(bigQueryClient.getReadTableSchema(any())).thenReturn(null);

    TableNotFoundException exception =
        assertThrows(
            TableNotFoundException.class,
            () -> Spark3Util.getSchemaOrThrow(bigQueryClient, config, null));

    assertThat(exception).hasMessageThat().isEqualTo("Table thelook1_ecommerce.orders not found");
  }

  @Test
  public void testTableProviderInferSchema_returnsNullWhenTableNotFound() {
    Spark31BigQueryTableProvider provider =
        new Spark31BigQueryTableProvider() {
          @Override
          protected Table getBigQueryTableInternal(Map<String, String> properties) {
            BigQueryClient bigQueryClient = mock(BigQueryClient.class);
            SparkBigQueryConfig config = mock(SparkBigQueryConfig.class);
            when(config.getTableId()).thenReturn(TableId.of("p", "d", "missing_table"));
            when(config.getQuery()).thenReturn(Optional.empty());
            when(bigQueryClient.getReadTableSchema(any())).thenReturn(null);
            Injector injector = mock(Injector.class);
            when(injector.getInstance(SparkBigQueryConfig.class)).thenReturn(config);
            return new Spark31BigQueryTable(
                injector, () -> Spark3Util.getSchemaOrThrow(bigQueryClient, config, null));
          }
        };

    // A null schema lets Spark call getTable(null, ...), which is what makes writing a DataFrame
    // to a brand new BigQuery table keep working.
    assertThat(provider.inferSchema(EMPTY_OPTIONS)).isNull();
    // Resolving the table's schema for a read still reports the missing table.
    TableNotFoundException exception =
        assertThrows(
            TableNotFoundException.class,
            () -> provider.getBigQueryTableInternal(EMPTY_OPTIONS).schema());
    assertThat(exception).hasMessageThat().isEqualTo("Table p.d.missing_table not found");
  }

  @Test
  public void testTableProviderInferSchema_doesNotDependOnTableNotFoundMessage() {
    Spark31BigQueryTableProvider provider =
        providerWithSchemaException(new TableNotFoundException("Missing table"));

    assertThat(provider.inferSchema(EMPTY_OPTIONS)).isNull();
  }

  @Test
  public void testTableProviderInferSchema_rethrowsUnrelatedNotFoundException() {
    BigQueryConnectorException original = new BigQueryConnectorException("Credentials not found");
    Spark31BigQueryTableProvider provider = providerWithSchemaException(original);

    BigQueryConnectorException exception =
        assertThrows(BigQueryConnectorException.class, () -> provider.inferSchema(EMPTY_OPTIONS));

    assertThat(exception).isSameInstanceAs(original);
  }

  @Test
  public void testTableProviderInferSchema_rethrowsOtherBigQueryConnectorException() {
    Spark31BigQueryTableProvider provider =
        new Spark31BigQueryTableProvider() {
          @Override
          protected Table getBigQueryTableInternal(Map<String, String> properties) {
            throw new BigQueryConnectorException("Authentication failed");
          }
        };

    BigQueryConnectorException exception =
        assertThrows(BigQueryConnectorException.class, () -> provider.inferSchema(EMPTY_OPTIONS));

    assertThat(exception).hasMessageThat().isEqualTo("Authentication failed");
  }

  private Spark31BigQueryTableProvider providerWithSchemaException(
      BigQueryConnectorException exception) {
    Table table = mock(Table.class);
    when(table.schema()).thenThrow(exception);
    return new Spark31BigQueryTableProvider() {
      @Override
      protected Table getBigQueryTableInternal(Map<String, String> properties) {
        return table;
      }
    };
  }
}
