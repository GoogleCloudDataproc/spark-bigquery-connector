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
package com.google.cloud.spark.bigquery.write;

import static com.google.cloud.spark.bigquery.SparkBigQueryUtil.scalaMapToJavaMap;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.DataSourceVersion;
import com.google.cloud.spark.bigquery.InjectorBuilder;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.write.context.BigQueryDataSourceWriterModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;

public class CreatableRelationProviderHelper {

  public BaseRelation createRelation(
      SQLContext sqlContext,
      SaveMode saveMode,
      scala.collection.immutable.Map<String, String> parameters,
      Dataset<Row> data,
      Map<String, String> customDefaults) {

    Map<String, String> properties = scalaMapToJavaMap(parameters);
    BigQueryInsertableRelationBase relation =
        createBigQueryInsertableRelation(sqlContext, data, properties, saveMode, customDefaults);

    switch (saveMode) {
      case Append:
        relation.insert(data, /* overwrite */ false);
        break;
      case Overwrite:
        relation.insert(data, /* overwrite */ true);
        break;
      case ErrorIfExists:
        if (!relation.exists()) {
          relation.insert(data, /* overwrite */ false);
          break;
        } else {
          throw new IllegalArgumentException(
              "SaveMode is set to ErrorIfExists and Table "
                  + BigQueryUtil.friendlyTableName(relation.getTableId())
                  + "already exists. Did you want to add data to the table by setting "
                  + "the SaveMode to Append? Example: "
                  + "df.write.format.options.mode(SaveMode.Append).save()");
        }
      case Ignore:
        if (!relation.exists()) {
          relation.insert(data, /* overwrite */ false);
          break;
        }
    }

    return relation;
  }

  @VisibleForTesting
  BigQueryInsertableRelationBase createBigQueryInsertableRelation(
      SQLContext sqlContext,
      Dataset<Row> data,
      Map<String, String> properties,
      SaveMode saveMode,
      Map<String, String> customDefaults) {
    Injector injector =
        new InjectorBuilder()
            .withDataSourceVersion(DataSourceVersion.V1)
            .withSpark(sqlContext.sparkSession())
            .withSchema(data.schema())
            .withOptions(properties)
            .withCustomDefaults(customDefaults)
            .withTableIsMandatory(true)
            .build();

    return createBigQueryInsertableRelationInternal(sqlContext, data, saveMode, injector);
  }

  public BigQueryInsertableRelationBase createBigQueryInsertableRelation(
      SQLContext sqlContext, Dataset<Row> data, SaveMode saveMode, SparkBigQueryConfig config) {
    Injector injector =
        new InjectorBuilder()
            .withDataSourceVersion(DataSourceVersion.V1)
            .withSpark(sqlContext.sparkSession())
            .withSchema(data.schema())
            .withConfig(config)
            .withTableIsMandatory(true)
            .build();

    return createBigQueryInsertableRelationInternal(sqlContext, data, saveMode, injector);
  }

  private BigQueryInsertableRelationBase createBigQueryInsertableRelationInternal(
      SQLContext sqlContext, Dataset<Row> data, SaveMode saveMode, Injector injector) {
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    BigQueryClient bigQueryClient = injector.getInstance(BigQueryClient.class);

    SparkBigQueryConfig.WriteMethod writeMethod = config.getWriteMethod();
    if (writeMethod == SparkBigQueryConfig.WriteMethod.INDIRECT) {
      return new BigQueryDeprecatedIndirectInsertableRelation(bigQueryClient, sqlContext, config);
    }
    // Need DataSourceWriterContext
    Injector writerInjector =
        injector.createChildInjector(
            new BigQueryDataSourceWriterModule(
                config, UUID.randomUUID().toString(), data.schema(), saveMode));
    return new BigQueryDataSourceWriterInsertableRelation(
        bigQueryClient, sqlContext, config, writerInjector);
  }
}
