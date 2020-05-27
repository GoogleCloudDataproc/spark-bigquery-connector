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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class IntegrationTestUtils {

    static Logger logger = LoggerFactory.getLogger(IntegrationTestUtils.class);

    public static BigQuery getBigquery() {
        return BigQueryOptions.getDefaultInstance().getService();
    }

    public static void createDataset(String dataset) {
        BigQuery bq = getBigquery();
        DatasetId datasetId = DatasetId.of(dataset);
        logger.warn("Creating test dataset: {}", datasetId);
        bq.create(DatasetInfo.of(datasetId));
    }

    public static void runQuery(String query) {
        BigQueryClient bigQueryClient = new BigQueryClient(getBigquery(), Optional.empty(), Optional.empty());
        logger.warn("Running query '{}'", query);
        bigQueryClient.query(query);
    }

    public static void deleteDatasetAndTables(String dataset) {
        BigQuery bq = getBigquery();
        logger.warn("Deleting test dataset '{}' and its contents", dataset);
        bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
    }


}
