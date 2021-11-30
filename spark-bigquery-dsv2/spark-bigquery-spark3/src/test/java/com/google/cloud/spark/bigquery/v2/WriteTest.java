package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WriteTest {

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
    //    Dataset<Row> df =
    //        spark
    //            .createDataset(
    //                Arrays.asList( //
    //                    new Person(
    //                        "Abc",
    //                        Arrays.asList(new Friend(10, Arrays.asList(new
    // Link("www.abc.com"))))), //
    //                    new Person(
    //                        "Def",
    //                        Arrays.asList(new Friend(12, Arrays.asList(new
    // Link("www.def.com")))))),
    //                Encoders.bean(Person.class))
    //            .toDF();
    //    df.show();
    //    df.write()
    //        .format("bigquery")
    //        .option("table", "tidy-tine-318906.wordcount_dataset.sample")
    //        .option("schema", df.schema().toDDL())
    //        .option("temporaryGcsBucket", "sample-allan-bucket")
    //        .mode(SaveMode.Append)
    //        .save();
    StructType schemaStructType =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("age", DataTypes.LongType, true)
            });
    Dataset<Row> readDf =
        spark
            .read()
            .option("table", "tidy-tine-318906.wordcount_dataset.sample")
            //                        .option("inferSchema","true")
            .format("bigquery")
            .load();
    readDf.show();
  }
}
