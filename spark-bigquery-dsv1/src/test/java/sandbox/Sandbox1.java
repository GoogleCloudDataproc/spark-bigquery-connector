package sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class Sandbox1 {

  @Test
  public void test1() throws Exception {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
    Dataset<Row> df = spark.read().format("bigquery")
        .load("bigquery-public-data.samples.shakespeare");
    df.show();
    Dataset<String> sort = df.select("word")
        .where("word >= 'a' AND word not like '%\\'%'")
        .distinct()
        .as(Encoders.STRING())
        .sort("word");
    Object obj = sort.take(4);
    System.out.println(obj);

  }

}
