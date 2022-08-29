package a;

import java.util.ArrayList;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

public class FooRDD extends RDD<InternalRow> {

  public FooRDD(SparkContext sc) {
    super(
        sc,
        // (Seq<Dependency<?>>) Seq$.MODULE$.empty(),
        (Seq<Dependency<?>>) JavaConverters.asScalaBuffer(new ArrayList<Dependency<?>>()).toSeq(),
        scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));
    System.out.println("after super");
  }

  @Override
  public Iterator<InternalRow> compute(Partition split, TaskContext context) {
    return null;
  }

  @Override
  public Partition[] getPartitions() {
    return new Partition[0];
  }

  @Override
  public String toString() {
    return "FooRDD";
  }

  public static void main(String[] args) {
    SparkContext sc = SparkContext.getOrCreate();
    FooRDD fooRDD = new FooRDD(sc);
    System.out.println(fooRDD);
  }
}
