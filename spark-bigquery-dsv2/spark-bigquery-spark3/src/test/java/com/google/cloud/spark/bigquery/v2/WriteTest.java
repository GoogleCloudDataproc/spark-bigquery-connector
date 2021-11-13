package com.google.cloud.spark.bigquery.v2;

import java.io.Serializable;
import java.util.Arrays;

import com.google.cloud.spark.bigquery.integration.model.Friend;
import com.google.cloud.spark.bigquery.integration.model.Link;
import com.google.cloud.spark.bigquery.integration.model.Person;
import org.apache.spark.sql.*;

public class WriteTest implements Serializable {

  private static SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSparkSession = SparkSession.getActiveSession();
    if (defaultSparkSession.isDefined()) {
      return defaultSparkSession.get();
    }
    return SparkSession.builder()
        .appName("spark-bigquery-connector")
        .master("local[*]")
        .getOrCreate();
  }

  public static void main(String[] args) {
    SparkSession spark = WriteTest.getDefaultSparkSessionOrCreate();
        Dataset<Row> df =
            spark
                .createDataset(
                    Arrays.asList( //
                        new Person(
                            "Abc",
                            Arrays.asList(new Friend(10, Arrays.asList(new
                                    Link("www.abc.com"))))), //
                        new Person(
                            "Def",
                            Arrays.asList(new Friend(12, Arrays.asList(new
     Link("www.def.com")))))),
                    Encoders.bean(Person.class))
                .toDF();
        df.write()
            .format("bigquery")
            .option("table", "tidy-tine-318906.wordcount_dataset.sample")
            .option("schema", df.schema().toDDL())
            .option("temporaryGcsBucket", "sample-allan-bucket")
            .mode(SaveMode.Overwrite)
            .save();

    Dataset<Row> readDf =
        spark
            .read()
            .option("table", "tidy-tine-318906.wordcount_dataset.sample")
            .format("bigquery")
            .option("readDataFormat", "AVRO")
            .load();
    readDf.show();

        readDf
                .write()
                .format("bigquery")
                .option("table", "tidy-tine-318906.wordcount_dataset.sample")
                .option("schema", readDf.schema().toDDL())
                .option("temporaryGcsBucket", "sample-allan-bucket")
                .mode(SaveMode.Append)
                .save();

      System.out.println(readDf.count());
  }
}
