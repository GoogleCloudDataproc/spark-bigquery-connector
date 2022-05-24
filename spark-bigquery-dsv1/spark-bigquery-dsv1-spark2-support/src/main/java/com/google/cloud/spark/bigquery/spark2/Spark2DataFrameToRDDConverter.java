/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.spark.bigquery.spark2;

import com.google.cloud.spark.bigquery.DataFrameToRDDConverter;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Spark2DataFrameToRDDConverter implements DataFrameToRDDConverter {

  @Override
  public RDD<Row> convertToRDD(Dataset<Row> data) {
    StructType schema = data.schema();
    List<Attribute> attributes =
        JavaConverters.asJavaCollection(schema.toAttributes()).stream()
            .map(Attribute::toAttribute)
            .collect(Collectors.toList());

    final ExpressionEncoder<Row> expressionEncoder =
        RowEncoder.apply(schema)
            .resolveAndBind(
                JavaConverters.asScalaIteratorConverter(attributes.iterator()).asScala().toSeq(),
                SimpleAnalyzer$.MODULE$);

    RDD<Row> rowRdd =
        data.queryExecution()
            .toRdd()
            .toJavaRDD()
            .mapPartitions(iter -> new EncodingIterator(iter, expressionEncoder))
            .rdd();

    return rowRdd;
  }

  static class EncodingIterator implements Iterator<Row> {
    private Iterator<InternalRow> internalIterator;
    private ExpressionEncoder<Row> expressionEncoder;

    public EncodingIterator(
        Iterator<InternalRow> internalIterator, ExpressionEncoder<Row> expressionEncoder) {
      this.internalIterator = internalIterator;
      this.expressionEncoder = expressionEncoder;
    }

    @Override
    public boolean hasNext() {
      return internalIterator.hasNext();
    }

    @Override
    public Row next() {
      return expressionEncoder.fromRow(internalIterator.next());
    }
  }
}
