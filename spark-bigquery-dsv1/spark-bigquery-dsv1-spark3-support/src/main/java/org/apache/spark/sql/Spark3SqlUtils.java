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
package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Spark3SqlUtils {

  private Spark3SqlUtils() {}

  // `toAttributes` is protected[sql] starting spark 3.2.0, so we need this call to be in the same
  // package. Since Scala 2.13/Spark 3.3 forbids it, the implementation has been ported to Java
  public static Seq<AttributeReference> toAttributes(StructType schema) {
    List<AttributeReference> result =
        Stream.of(schema.fields())
            .map(
                field ->
                    new AttributeReference(
                        field.name(),
                        field.dataType(),
                        field.nullable(),
                        field.metadata(),
                        NamedExpression.newExprId(),
                        Seq$.MODULE$.<String>newBuilder().result()))
            .collect(Collectors.toList());
    return JavaConverters.asScalaBuffer(result).toSeq();
  }
}
