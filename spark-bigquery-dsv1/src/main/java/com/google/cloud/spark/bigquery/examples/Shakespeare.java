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
package com.google.cloud.spark.bigquery.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Shakespeare {
  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder()
            .appName("spark-bigquery-demo-java")
            // .master("local[*]") // Uncomment for local testing if not set by spark-submit
            .getOrCreate();

    // Use the Cloud Storage bucket for temporary BigQuery export data used
    // by the connector. This assumes the Cloud Storage connector for
    // Hadoop is configured.
    String bucket = spark.sparkContext().hadoopConfiguration().get("fs.gs.system.bucket");
    if (bucket == null || bucket.isEmpty()) {
      // Fallback or error if the bucket is not configured
      // For the example, let's assume a default or throw an error.
      // System.err.println("GCS system bucket 'fs.gs.system.bucket' not configured. Please set it
      // for temporary data.");
      // For local testing, you might set a default:
      // bucket = "your-gcs-bucket"; // Replace with your actual bucket
      // Or make it a required argument.
      // For this direct translation, we'll just set it via conf if null for the example to proceed,
      // but in production, this should be properly configured.
      if (args.length > 0 && args[0] != null && !args[0].isEmpty()) {
        bucket = args[0]; // Expect bucket as first argument if not in Hadoop conf
      } else {
        System.err.println("Usage: Shakespeare <temporaryGcsBucket>");
        System.err.println(
            "Falling back to trying to read 'temporaryGcsBucket' from spark conf or hadoop conf.");
        // If still not found, the connector might throw an error later if it needs it.
      }
    }
    // If a bucket is provided (either from Hadoop conf or args), set it in Spark conf
    // The connector will pick it up from SparkConf or HadoopConf.
    // If 'temporaryGcsBucket' is explicitly set in parameters to read/write, that takes precedence.
    if (bucket != null && !bucket.isEmpty()) {
      spark.conf().set("temporaryGcsBucket", bucket);
    }

    // Load data in from BigQuery.
    // Using the static helper method from BigQueryDataFrames for the .bigquery() syntax
    Dataset<Row> wordsDF =
        spark.read().format("bigquery").load("bigquery-public-data.samples.shakespeare").cache();

    wordsDF.show();
    wordsDF.printSchema();
    wordsDF.createOrReplaceTempView("words");

    // Perform word count.
    Dataset<Row> wordCountDF =
        spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word");
    wordCountDF.show();

    // Saving the data to BigQuery
    // Usage: specify dataset and table.
    // Ensure 'wordcount_dataset' exists in your project or adjust the table name.
    // Example: "your_project:wordcount_dataset.wordcount_output_java"
    String outputTable = "wordcount_dataset.wordcount_output_java"; // Modify as needed
    if (args.length > 1 && args[1] != null && !args[1].isEmpty()) {
      outputTable = args[1]; // Expect output table as second argument
    } else {
      System.err.println(
          "Defaulting output table to: " + outputTable + ". Provide as second arg if needed.");
    }

    wordCountDF
        .write()
        .format("bigquery")
        .save(outputTable); // The table string here is just the table name, dataset.table
    // Project can be inherited from default-project-id or Spark conf.

    System.out.println("Word count results written to BigQuery table: " + outputTable);

    spark.stop();
  }
}
