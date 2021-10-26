package com.google.cloud.spark.bigquery.v2;

import java.io.PrintStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaShakespeare {

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().master("local").appName("spark-bigquery-demo").getOrCreate();

    String outputBigqueryTable = "wordcount_dataset.wordcount_output";
    if (args.length == 1) {
      outputBigqueryTable = args[0];
    } else if (args.length > 1) {
      usage();
    }

    // Use the Cloud Storage bucket for temporary BigQuery export data used
    // by the connector. This assumes the Cloud Storage connector for
    // Hadoop is configured.
    String bucket = spark.sparkContext().hadoopConfiguration().get("fs.gs.system.bucket");
    spark.conf().set("temporaryGcsBucket", "bigquery_export_data_bucket");
    spark.conf().set("project", "tidy-tine-318906");

    // Load data in from BigQuery.
    Dataset<Row> wordsDF =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .load()
            .cache();

    wordsDF.show();
    wordsDF.printSchema();
    wordsDF.createOrReplaceTempView("words");

    // Perform word count.
    Dataset<Row> wordCountDF =
        spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word");

    wordCountDF.show();
    wordCountDF.printSchema();

    // Saving the data to BigQuery
    wordCountDF.write().format("bigquery").option("table", outputBigqueryTable).save();
  }

  private static void usage() {
    PrintStream out = System.out;
    out.println("usage: spark [OUTPUT_BIGQUERY_TABLE]");
    out.println("[OUTPUT_BIGQUERY_TABLE]       Set the output bigquery table to ");
    out.println("                              OUTPUT_BIGQUERY_TABLE. By default the location");
    out.println("                              is wordcount_dataset.wordcount_output");
    System.exit(1);
  }
}
