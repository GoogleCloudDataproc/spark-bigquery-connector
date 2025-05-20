package com.google.cloud.spark.bigquery;

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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;

public class Scala212BigQueryRelationProvider extends BigQueryRelationProviderBase implements RelationProvider, CreatableRelationProvider, SchemaRelationProvider, StreamSinkProvider {
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        return null;
    }

    @Override
    public boolean supportsDataType(DataType dt) {
        return CreatableRelationProvider.super.supportsDataType(dt);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        return null;
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
        return null;
    }

    @Override
    public Sink createSink(SQLContext sqlContext, Map<String, String> parameters, Seq<String> partitionColumns, OutputMode outputMode) {
        return null;
    }