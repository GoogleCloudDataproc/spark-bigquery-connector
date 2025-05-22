package com.google.cloud.spark.bigquery;

import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class Scala212BigQueryRelationProvider extends BigQueryRelationProviderBase
    implements RelationProvider,
        CreatableRelationProvider,
        SchemaRelationProvider,
        StreamSinkProvider {

  public Scala212BigQueryRelationProvider() {
    super();
  }

  public Scala212BigQueryRelationProvider(Supplier<GuiceInjectorCreator> getGuiceInjectorCreator) {
    super(getGuiceInjectorCreator);
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
    return super.createRelation(sqlContext, mode, asJava(parameters), data);
  }

  @Override
  public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
    return super.createRelation(sqlContext, asJava(parameters));
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
    return super.createRelation(sqlContext, asJava(parameters), schema);
  }

  @Override
  public Sink createSink(
      SQLContext sqlContext,
      Map<String, String> parameters,
      Seq<String> partitionColumns,
      OutputMode outputMode) {
    return super.createSink(sqlContext, asJava(parameters), asJava(partitionColumns), outputMode);
  }

  private java.util.List<String> asJava(Seq<String> seq) {
    return JavaConverters.seqAsJavaListConverter(seq).asJava();
  }

  private java.util.Map<String, String> asJava(Map<String, String> map) {
    return JavaConverters.mapAsJavaMapConverter(map).asJava();
  }
}
