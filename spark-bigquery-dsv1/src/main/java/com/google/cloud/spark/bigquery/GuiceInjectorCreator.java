/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.inject.Injector;

import java.util.Map;
import java.util.Optional;

// Interface for GuiceInjectorCreator
interface GuiceInjectorCreator {
    default Injector createGuiceInjector(
            SQLContext sqlContext,
            Map<String, String> parameters,
            Optional<StructType> schema) {
        org.apache.spark.sql.SparkSession spark = sqlContext.sparkSession();
        return Guice.createInjector(
                new BigQueryClientModule(),
                new SparkBigQueryConnectorModule(
                        spark,
                        parameters,
                        ImmutableMap.of(), // Assuming empty java.util.Map for the third param
                        schema,
                        DataSourceVersion.V1,
                        true, // tableIsMandatory
                        java.util.Optional.empty()));
    }
}
