/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.bigquery.connector.common.BigQueryPushdownUnsupportedException
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{addAttributeStatement, blockStatement, makeStatement}
import org.apache.spark.bigquery.BigNumericUDT
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** This interface performs the conversion from Spark expressions to SQL runnable by BigQuery.
 * Spark Expressions are recursively pattern matched. Expressions that differ across Spark versions should be implemented in subclasses
 *
 */
trait SparkExpressionConverter {
  /**
   * Tries to convert Spark expressions by matching across the different families of expressions such as Aggregate, Boolean etc.
   * @param expression
   * @param fields
   * @return
   */
  def convertStatement(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    convertAggregateExpressions(expression, fields)
      .orElse(convertBasicExpressions(expression, fields))
      .orElse(convertBooleanExpressions(expression, fields))
      .orElse(convertMiscExpressions(expression, fields))
      .getOrElse(throw new BigQueryPushdownUnsupportedException((s"Pushdown unsupported for ${expression.prettyName}")))
  }

  def convertStatements(fields: Seq[Attribute], expressions: Expression*): BigQuerySQLStatement =
    makeStatement(expressions.map(convertStatement(_, fields)), ",")

  def convertAggregateExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    expression match {
      case _: AggregateExpression =>
        // Take only the first child, as all of the functions below have only one.
        expression.children.headOption.flatMap(agg_fun => {
          Option(agg_fun match {
            case _: Average | _: Corr | _: CovPopulation | _: CovSample | _: Count |
                 _: Max | _: Min | _: Sum | _: StddevPop | _: StddevSamp |
                 _: VariancePop | _: VarianceSamp =>
              val distinct: BigQuerySQLStatement =
                if (expression.sql contains "(DISTINCT ") ConstantString("DISTINCT").toStatement
                else EmptyBigQuerySQLStatement()

              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*)
                )
          })
        })
      case _ => None
    }
  }

  def convertBasicExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case a: Attribute => addAttributeStatement(a, fields)
      case And(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "AND" + convertStatement(right, fields)
        )
      case Or(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "OR" + convertStatement(right, fields)
        )
      case b: BinaryOperator =>
        blockStatement(
          convertStatement(b.left, fields) + b.symbol + convertStatement(b.right, fields)
        )
      case l: Literal =>
        // TODO: Add DateType and TimestampType
        l.dataType match {
          case StringType =>
            if (l.value == null) {
              ConstantString("NULL").toStatement
            } else {
              StringVariable(Some(l.toString())).toStatement
            }
          case _ =>
            l.value match {
              case v: Int => IntVariable(Some(v)).toStatement
              case v: Long => LongVariable(Some(v)).toStatement
              case v: Short => ShortVariable(Some(v)).toStatement
              case v: Boolean => BooleanVariable(Some(v)).toStatement
              case v: Float => FloatVariable(Some(v)).toStatement
              case v: Double => DoubleVariable(Some(v)).toStatement
              case v: Byte => ByteVariable(Some(v)).toStatement
              case _ => ConstantStringVal(l.value).toStatement
            }
        }

      case _ => null
    })
  }

  def convertBooleanExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case In(child, list) if list.forall(_.isInstanceOf[Literal]) =>
        convertStatement(child, fields) + "IN" +
          blockStatement(convertStatements(fields, list: _*))
      case IsNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NULL")
      case IsNotNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NOT NULL")
      case Not(child) => {
        child match {
          case EqualTo(left, right) =>
            blockStatement(
              convertStatement(left, fields) + "!=" +
                convertStatement(right, fields)
            )
          case GreaterThanOrEqual(left, right) =>
            convertStatement(LessThan(left, right), fields)
          case LessThanOrEqual(left, right) =>
            convertStatement(GreaterThan(left, right), fields)
          case GreaterThan(left, right) =>
            convertStatement(LessThanOrEqual(left, right), fields)
          case LessThan(left, right) =>
            convertStatement(GreaterThanOrEqual(left, right), fields)
          case _ =>
            ConstantString("NOT") +
              blockStatement(convertStatement(child, fields))
        }
      }
      case Contains(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("CONTAINS_SUBSTR") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString}%'")
      case EndsWith(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("ENDS_WITH") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString}%'")
      case StartsWith(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("STARTS_WITH") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString}%'")

      case _ => null
    })
  }

  def convertMiscExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case Alias(child: Expression, name: String) =>
        blockStatement(convertStatement(child, fields), name)
      case SortOrder(child, Ascending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC"
      case SortOrder(child, Descending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC"
      case Cast(child, t, _) =>
        getCastType(t) match {
          case Some(cast) =>

            /** For known unsupported data conversion, raise exception to break the pushdown process.
             * For example, BigQuery doesn't support to convert DATE/TIMESTAMP to NUMBER
             */
            (child.dataType, t) match {
              case (_: DateType | _: TimestampType,
              _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) => {
                throw new BigQueryPushdownUnsupportedException(
                  "Pushdown failed due to unsupported conversion")
              }
              case _ =>
            }

            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }

      case _ => null
    })
  }

  /** Attempts a best effort conversion from a SparkType
   * to a BigQuery type to be used in a Cast.
   */
  final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType => "STRING"
      case ByteType => "BYTES"
      case BooleanType => "BOOL"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case d: DecimalType => "BIGDECIMAL(" + d.precision + ", " + d.scale + ")"
      case IntegerType | ShortType | LongType => "INT64"
      case FloatType | DoubleType => "FLOAT64"
      case _ => null
    })
}
