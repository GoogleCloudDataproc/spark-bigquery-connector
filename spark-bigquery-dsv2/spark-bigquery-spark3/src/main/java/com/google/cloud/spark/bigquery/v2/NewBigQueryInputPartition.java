package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.common.GenericBigQueryInputPartition;
import org.apache.spark.sql.connector.read.InputPartition;

public class NewBigQueryInputPartition extends GenericBigQueryInputPartition implements InputPartition {

    public NewBigQueryInputPartition(BigQueryReadClientFactory bigQueryReadClientFactory,
                                     String streamName,
                                     ReadRowsHelper.Options options,
                                     ReadRowsResponseToInternalRowIteratorConverter converter) {
        super(bigQueryReadClientFactory, streamName, options, converter);
    }
}
