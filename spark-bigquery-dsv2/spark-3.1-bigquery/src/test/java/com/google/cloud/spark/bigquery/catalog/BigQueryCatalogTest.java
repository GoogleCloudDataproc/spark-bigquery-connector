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
package com.google.cloud.spark.bigquery.catalog;

import static com.google.cloud.bigquery.LegacySQLTypeName.STRING;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.v2.BigQueryTable;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Before;
import org.junit.Test;

public class BigQueryCatalogTest {

  static final StandardTableDefinition BQ_TABLE_DEF =
      StandardTableDefinition.newBuilder()
          .setSchema(Schema.of(Field.newBuilder("name", STRING).setMode(Mode.NULLABLE).build()))
          .build();

  BigQueryClient bigQueryClient = mock(BigQueryClient.class);
  BigQueryCatalog catalog;

  @Before
  public void setUp() {
    SparkSession unused = SparkSession.builder().master("local[1]").getOrCreate();
    catalog = new BigQueryCatalog(bigQueryClient);
  }

  @Test
  public void testName() {
    catalog.initialize("foo", new CaseInsensitiveStringMap(new HashMap()));
    assertThat(catalog.name()).isEqualTo("foo");
  }

  @Test
  public void testCreateTableSucceeds() throws TableAlreadyExistsException {
    when(bigQueryClient.createTable(any(), any()))
        .thenReturn(TableInfo.of(TableId.of("dataset", "table"), BQ_TABLE_DEF));
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(DataTypes.createStructField("name", DataTypes.StringType, false)));

    catalog.initialize("foo", new CaseInsensitiveStringMap(new HashMap()));
    BigQueryTable created =
        catalog.createTable(
            Identifier.of(new String[] {"dataset"}, "table"),
            sparkSchema,
            new Transform[] {},
            /*properties=*/ new HashMap());
    assertThat(created.name()).isEqualTo("table");
    assertThat(created.schema().simpleString()).isEqualTo(sparkSchema.simpleString());
  }

  @Test
  public void testDropTableSucceeds() {
    when(bigQueryClient.deleteTable(any())).thenReturn(true);

    catalog.initialize("foo", new CaseInsensitiveStringMap(new HashMap()));
    assertThat(catalog.dropTable(Identifier.of(new String[] {"dataset"}, "table"))).isEqualTo(true);
  }

  @Test
  public void testListTablesSucceeds() {
    TableInfo table1 = TableInfo.of(TableId.of("dataset", "table1"), BQ_TABLE_DEF);
    TableInfo table2 = TableInfo.of(TableId.of("dataset", "table2"), BQ_TABLE_DEF);
    when(bigQueryClient.listTables(any(), any(), any(), any(), any()))
        .thenReturn((Iterable<TableInfo>) Arrays.asList(table1, table2));

    catalog.initialize("foo", new CaseInsensitiveStringMap(new HashMap()));
    Identifier[] tables = catalog.listTables(new String[] {"dataset"});

    assertThat(tables[0].name()).isEqualTo("table1");
    assertThat(tables[1].name()).isEqualTo("table2");
  }

  @Test
  public void testLoadTableSucceeds() throws NoSuchTableException {
    when(bigQueryClient.getTable(any()))
        .thenReturn(TableInfo.of(TableId.of("dataset", "table"), BQ_TABLE_DEF));

    catalog.initialize("foo", new CaseInsensitiveStringMap(new HashMap()));
    BigQueryTable returned = catalog.loadTable(Identifier.of(new String[] {"dataset"}, "table"));

    assertThat(returned.name()).isEqualTo("table");
  }
}
