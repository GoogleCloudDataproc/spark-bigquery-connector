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

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.formatTableResult;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.InjectorBuilder;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.write.CreatableRelationProviderHelper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.openlineage.spark.shade.client.OpenLineage;
import io.openlineage.spark.shade.client.utils.DatasetIdentifier;
import io.openlineage.spark.shade.extension.v1.LineageRelationProvider;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.connector.ExternalCommandRunner;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

public class Spark31BigQueryTableProvider extends BaseBigQuerySource
    implements TableProvider,
        CreatableRelationProvider,
        LineageRelationProvider,
        ExternalCommandRunner {

  private static final Transform[] EMPTY_TRANSFORM_ARRAY = {};

  /**
   * Returns the schema of the BigQuery table, or {@code null} if the table does not exist yet.
   *
   * <p>As {@link #supportsExternalMetadata()} is {@code false}, Spark calls this method before
   * {@link #getTable} on both the read and the write paths. Returning {@code null} for a missing
   * table keeps {@code df.write.format("bigquery").save()} working against a table that has yet to
   * be created: Spark passes the {@code null} on to {@code getTable}, and since the table only
   * advertises {@code V1_BATCH_WRITE} the write falls back to the V1 {@link
   * org.apache.spark.sql.sources.CreatableRelationProvider} path, which never resolves the schema.
   * On the read path Spark does resolve it, and {@code Table.schema()} then surfaces the same
   * exception with a readable message.
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    try {
      return getBigQueryTableInternal(options).schema();
    } catch (TableNotFoundException e) {
      return null;
    }
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return Spark3Util.createBigQueryTableInstance(Spark31BigQueryTable::new, schema, properties);
  }

  protected Table getBigQueryTableInternal(Map<String, String> properties) {
    return Spark3Util.createBigQueryTableInstance(Spark31BigQueryTable::new, null, properties);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      SaveMode mode,
      scala.collection.immutable.Map<String, String> parameters,
      Dataset<Row> data) {
    return new CreatableRelationProviderHelper()
        .createRelation(sqlContext, mode, parameters, data, ImmutableMap.of());
  }

  @Override
  public DatasetIdentifier getLineageDatasetIdentifier(
      String sparkListenerEventName,
      OpenLineage openLineage,
      Object sqlContext,
      Object parameters) {
    Map<String, String> properties = JavaConverters.mapAsJavaMap((CaseInsensitiveMap) parameters);
    Injector injector = new InjectorBuilder().withOptions(properties).build();
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    TableId tableId = config.getTableIdWithExplicitProject();
    return new DatasetIdentifier(BigQueryUtil.friendlyTableName(tableId), "bigquery");
  }

  @Override
  public String[] executeCommand(String command, CaseInsensitiveStringMap options) {
    String trimmedCommand = command.trim().toUpperCase(Locale.ROOT);
    if (trimmedCommand.startsWith("SELECT") || trimmedCommand.startsWith("WITH")) {
      throw new IllegalArgumentException(
          "SELECT and WITH statements are not supported for EXECUTE IMMEDIATE. "
              + "Please use spark.read.format(\"bigquery\").load(command) instead.");
    }
    Injector injector =
        new InjectorBuilder().withTableIsMandatory(false).withOptions(options).build();
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    TableResult result = bqClient.query(command);
    return formatTableResult(result, /* withHeader */ false);
  }
}
