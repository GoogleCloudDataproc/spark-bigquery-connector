/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import static scala.collection.JavaConversions.mapAsJavaMap;

public class BigQueryDataSourceV2 implements DataSourceV2, ReadSupport {

    @Override
    public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
        SparkSession spark = getDefaultSparkSessionOrCreate();

        SparkBigQueryConfig config = SparkBigQueryConfig.from(options,
                ImmutableMap.copyOf(mapAsJavaMap(spark.conf().getAll())),
                spark.sparkContext().hadoopConfiguration());

        Injector injector = Guice.createInjector(
                new BigQueryClientModule(),
                new SparkBigQueryConnectorModule(
                        spark.sparkContext(),
                        config.getTableId(),
                        config.toReadSessionCreatorConfig(),
                        schema));

        BigQueryDataSourceReader reader = injector.getInstance(BigQueryDataSourceReader.class);
        return reader;
    }

    private SparkSession getDefaultSparkSessionOrCreate() {
        scala.Option<SparkSession> defaultSpareSession = SparkSession.getDefaultSession();
        if (defaultSpareSession.isDefined()) {
            return defaultSpareSession.get();
        }
        return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return createReader(null, options);
    }
}

