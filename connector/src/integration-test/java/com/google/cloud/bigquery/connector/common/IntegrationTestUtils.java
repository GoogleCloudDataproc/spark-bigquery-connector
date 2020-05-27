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
