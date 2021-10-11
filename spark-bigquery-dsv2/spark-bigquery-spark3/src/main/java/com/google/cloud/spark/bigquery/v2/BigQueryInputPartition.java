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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import org.apache.spark.sql.connector.read.InputPartition;

public class BigQueryInputPartition implements InputPartition {

    private BigQueryReadClientFactory bigQueryReadClientFactory;
    private String streamName;
    private ReadRowsHelper.Options options;
    private ReadRowsResponseToInternalRowIteratorConverter converter;

    public BigQueryInputPartition() {
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.streamName = streamName;
        this.options = options;
        this.converter = converter;
    }
}
