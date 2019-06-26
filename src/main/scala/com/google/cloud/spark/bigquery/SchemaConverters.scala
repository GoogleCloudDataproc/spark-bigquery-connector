/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery

import java.nio.ByteBuffer

import com.google.cloud.bigquery.LegacySQLTypeName._
import com.google.cloud.bigquery.{Field, LegacySQLTypeName, Schema}
import com.typesafe.scalalogging.Logger
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/** Stateless converters for Converting between Spark and BigQuery types. */
object SchemaConverters {

  private val log = Logger(getClass)

  // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
  private val BQ_NUMERIC_PRECISION = 38
  private val BQ_NUMERIC_SCALE = 9
  private lazy val NUMERIC_SPARK_TYPE = DataTypes.createDecimalType(
    BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE)

  /** Convert a BigQuery schema to a Spark schema */
  def toSpark(schema: Schema): StructType = {
    def convert(field: Field): StructField = {
      var dataType = field.getType match {
        // TODO(#1): Support NUMERIC
        case INTEGER => LongType
        case FLOAT => DoubleType
        case NUMERIC => NUMERIC_SPARK_TYPE
        case STRING => StringType
        case BOOLEAN => BooleanType
        case BYTES => BinaryType
        case DATE => DateType
        case TIMESTAMP => TimestampType
        case TIME => LongType
        // TODO(#5): add a timezone to allow parsing to timestamp
        // This can be safely cast to TimestampType, but doing so causes the date to be inferred
        // as the current date. It's safer to leave as a stable string and give the user the
        // option of casting themselves.
        case DATETIME => StringType
        case RECORD => StructType(field.getSubFields.asScala.map(convert))
        case other => throw new IllegalArgumentException(s"Unsupported type '$other'")
      }
      var nullable = true
      Option(field.getMode) match {
        case Some(Field.Mode.REQUIRED) => nullable = false
        case Some(Field.Mode.REPEATED) => dataType = ArrayType(dataType, containsNull = false)
        case _ => () // nullable field
      }
      StructField(field.getName, dataType, nullable)
    }


    val res = StructType(schema.getFields.asScala.map(convert))
    log.debug(s"BQ Schema:\n'${schema.toString}'\n\nSpark schema:\n${res.treeString}")
    res
  }


  /**
   * Create a function that converts an Avro row with the given BigQuery schema to a Spark SQL row
   *
   * The conversion is based on the BigQuery schema, not Avro Schema, because the Avro schema is
   * very painful to use.
   *
   * Not guaranteed to be stable across all versions of Spark.
   */
  def createRowConverter(schema: Schema, namesInOrder: Seq[String])(record: GenericRecord)
  : InternalRow = {
    def convert(field: Field, value: Any): Any = {
      if (value == null) {
        return null
      }
      if (field.getMode == Field.Mode.REPEATED) {
        // rather than recurring down we strip off the repeated mode
        // Due to serialization issues, reconstruct the type using reflection:
        // See: https://github.com/googleapis/google-cloud-java/issues/3942
        val fType = LegacySQLTypeName.valueOfStrict(field.getType.name)
        val nestedField = Field.newBuilder(field.getName, fType, field.getSubFields)
            // As long as this is not repeated it works, but technically arrays cannot contain
            // nulls, so select required instead of nullable.
            .setMode(Field.Mode.REQUIRED)
            .build
        return new GenericArrayData(
          value.asInstanceOf[java.lang.Iterable[AnyRef]].asScala
              .map(v => convert(nestedField, v)))
      }
      field.getType match {
        case INTEGER | FLOAT | BOOLEAN | DATE | TIME | TIMESTAMP => value
        // TODO(pmkc): use String for safety?
        case STRING | DATETIME => UTF8String.fromBytes(value.asInstanceOf[Utf8].getBytes)
        case BYTES => getBytes(value.asInstanceOf[ByteBuffer])
        case NUMERIC =>
          val bytes = getBytes(value.asInstanceOf[ByteBuffer])
          Decimal(BigDecimal(BigInt(bytes), BQ_NUMERIC_SCALE), BQ_NUMERIC_PRECISION,
            BQ_NUMERIC_SCALE)
        case RECORD =>
          val fields = field.getSubFields.asScala
          convertAll(fields, value.asInstanceOf[GenericRecord], fields.map(_.getName))
        case other => throw new IllegalArgumentException(s"Unsupported type '$other'")
      }
    }

    def getBytes(buf: ByteBuffer) = {
      val bytes = new Array[Byte](buf.remaining)
      buf.get(bytes)
      bytes
    }

    // Schema is not recursive so add helper for sequence of fields
    def convertAll(fields: Seq[Field],
                   record: GenericRecord,
                   namesInOrder: Seq[String]): GenericInternalRow = {
      val getValue = fields.zip(Range(0, record.getSchema.getFields.size()).map(record.get))
          .map { case (field, value) => field.getName -> convert(field, value) }
          .toMap
      new GenericInternalRow(namesInOrder.map(getValue).toArray)
    }

    // Output in the order Spark expects
    convertAll(schema.getFields.asScala, record, namesInOrder)
  }
}

