package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.VersionProvider;
import org.apache.spark.SparkContext;
import scala.util.Properties;

import static java.lang.String.format;

public class SparkBigQueryConnectorVersionProvider implements VersionProvider {

    private SparkContext sparkContext;

    public SparkBigQueryConnectorVersionProvider(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @Override
    public String getVersion() {
        return format("spark-bigquery-connector/%s spark/%s java/%s scala/%s",
                BuildInfo.version(),
                sparkContext.version(),
                System.getProperty("java.runtime.version"),
                Properties.versionNumberString()
        );
    }
}
