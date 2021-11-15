package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StreamWriteExample {

  public static void main(String[] args) throws Exception {

    System.setProperty("hadoop.home.dir", "c:\\winutil\\");

    //   BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    SparkSession spark =
        SparkSession.builder()
            .master("local")
            .appName("spark-bigquery-demo")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();

    String bucket = "bigquery_export_data_bucket";
    spark
        .sparkContext()
        .hadoopConfiguration()
        .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    spark
        .sparkContext()
        .hadoopConfiguration()
        .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    spark.sparkContext().hadoopConfiguration().set("fs.gs.project.id", "tidy-tine-318906");
    spark
        .sparkContext()
        .hadoopConfiguration()
        .set("google.cloud.auth.service.account.enable", "true");
    spark.conf().set("temporaryGcsBucket", bucket);

    //  System.out.println("Created session" + spark);
    // read from socket

    spark.sparkContext().setLogLevel("ERROR");
    Dataset df =
        spark
            .readStream()
            .format("socket")
            .option("host", "localhost")
            .option("port", 50050)
            // .schema(schema)
            .load();

    System.out.println(df);

    //        DataStreamWriter writeDf = df.writeStream()
    //                .format("console")
    //                .outputMode(OutputMode.Append());

    DataStreamWriter writeDf =
        df.writeStream()
            .format("bigquery")
            .option("checkpointLocation", "/tmp")
            .option("table", "tidy-tine-318906.TestData.TestBqTable")
            .option("temporaryGcsBucket", bucket)
            .outputMode(OutputMode.Append());

    StreamingQuery query;

    try {
      query = writeDf.start();
      query.awaitTermination();
      query.stop();
    } catch (Exception e) {

      System.out.println("ERror");
      throw e;
    }
  }
}
