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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** This interface performs the conversion from Spark expressions to SQL runnable by BigQuery.
 * Spark Expressions are recursively pattern matched. Expressions that differ across Spark versions should be implemented in subclasses
 *
 */
abstract class SparkExpressionConverter {
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
      .orElse(convertDateExpressions(expression, fields))
      .orElse(convertMathematicalExpressions(expression, fields))
      .orElse(convertMiscellaneousExpressions(expression, fields))
      .orElse(convertStringExpressions(expression, fields))
      .orElse(convertWindowExpressions(expression, fields))
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
      case BitwiseAnd(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "&" + convertStatement(right, fields)
        )
      case BitwiseOr(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "|" + convertStatement(right, fields)
        )
      case BitwiseXor(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "^" + convertStatement(right, fields)
        )
      case BitwiseNot(child) =>
        ConstantString("~") + blockStatement(
          convertStatement(child, fields)
        )
      case EqualNullSafe(left, right) =>

        /**
         * Since NullSafeEqual operator is not supported in BigQuery, we are instead converting the operator to COALESCE ( CAST (leftExpression AS STRING), "" ) = COALESCE ( CAST (rightExpression AS STRING), "" )
         * Casting the expression to String to make sure the COALESCE arguments are of same type.
         */
        blockStatement(
          ConstantString("COALESCE") + blockStatement( ConstantString("CAST") + blockStatement(convertStatement(left, fields) + ConstantString("AS STRING") ) + "," + ConstantString("\"\"") ) +
            ConstantString("=") +
            ConstantString("COALESCE") + blockStatement( ConstantString("CAST") + blockStatement(convertStatement(right, fields) + ConstantString("AS STRING") ) + "," + ConstantString("\"\"") )
        )

      case b: BinaryOperator =>
        blockStatement(
          convertStatement(b.left, fields) + b.symbol + convertStatement(b.right, fields)
        )
      case l: Literal =>
        l.dataType match {
          case StringType =>
            if (l.value == null) {
              ConstantString("NULL").toStatement
            } else {
              StringVariable(Some(l.toString())).toStatement
            }
          case DateType =>
            ConstantString("DATE_ADD(DATE \"1970-01-01\", INTERVAL ") + IntVariable(
              Option(l.value).map(_.asInstanceOf[Int])
            ) + " DAY)" // s"DATE_ADD(DATE "1970-01-01", INTERVAL ${l.value} DAY)
          case TimestampType =>
            ConstantString("TIMESTAMP_MICROS(") + l.toString() + ")"
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
      case In(child, list) =>
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
        ConstantString("CONTAINS_SUBSTR") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString}'")

      case EndsWith(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("ENDS_WITH") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString}'")

      case StartsWith(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("STARTS_WITH") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString}'")

      case _ => null
    })
  }

  def convertDateExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case DateAdd(startDate, days) =>
        ConstantString(expression.prettyName.toUpperCase) +
          blockStatement(
              convertStatement(startDate, fields) + ", INTERVAL " +
              convertStatement(days, fields) + "DAY"
          )
      case DateSub(startDate, days) =>
        ConstantString(expression.prettyName.toUpperCase) +
          blockStatement(
              convertStatement(startDate, fields) + ", INTERVAL " +
              convertStatement(days, fields) + "DAY"
          )
      case Month(child) =>
        ConstantString("EXTRACT") +
          blockStatement(
            ConstantString(expression.prettyName.toUpperCase) + " FROM " +
              convertStatement(child, fields)
          )
      case Quarter(child) =>
        ConstantString("EXTRACT") +
          blockStatement(
            ConstantString(expression.prettyName.toUpperCase) + " FROM " +
              convertStatement(child, fields)
          )
      case Year(child) =>
        ConstantString("EXTRACT") +
          blockStatement(
            ConstantString(expression.prettyName.toUpperCase) + " FROM " +
              convertStatement(child, fields)
          )
      case TruncDate(date, format) =>
        ConstantString("DATE_TRUNC") +
          blockStatement(
            convertStatement(date, fields) + s", ${format.toString()}"
          )

      case _ => null
    })
  }

  def convertMathematicalExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case _: Abs | _: Acos | _: Asin | _: Atan |
           _: Cos | _: Cosh | _: Exp | _: Floor | _: Greatest |
           _: Least | _:Log10 | _: Pow | _:Round | _: Sin | _: Sinh |
           _: Sqrt | _: Tan | _: Tanh =>
        ConstantString(expression.prettyName.toUpperCase) + blockStatement(convertStatements(fields, expression.children: _*))

      case IsNaN(child) =>
        ConstantString("IS_NAN") + blockStatement(convertStatement(child, fields))

      case Signum(child) =>
        ConstantString("SIGN") + blockStatement(convertStatement(child, fields))

      case _: Rand =>
        ConstantString("RAND") + ConstantString("()")

      case Logarithm(base, expr) =>
        // In spark it is LOG(base,expr) whereas in BigQuery it is LOG(expr, base)
        ConstantString("LOG") + blockStatement(convertStatement(expr, fields) + "," + convertStatement(base, fields))

      case _: CheckOverflow =>
        convertCheckOverflowExpression(expression, fields)

      case Pi() => ConstantString("bqutil.fn.pi()").toStatement

      case PromotePrecision(child) => convertStatement(child, fields)

      case _: UnaryMinus =>
        convertUnaryMinusExpression(expression, fields)

      case _ => null
    })
  }

  def convertMiscellaneousExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
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

            /**
             * For known unsupported data conversion, raise exception to break the pushdown process.
             * For example, BigQuery doesn't support to convert DATE/TIMESTAMP to NUMBER
             */
            (child.dataType, t) match {
              case (_: DateType | _: TimestampType,
              _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) => {
                throw new BigQueryPushdownUnsupportedException(
                  "Pushdown failed due to unsupported conversion")
              }

              /**
               * BigQuery doesn't support casting from Integer to Bytes (https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#cast_as_bytes)
               * So handling this case separately.
               */
              case (_: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType ,_: ByteType) =>
                ConstantString("CAST") +
                  blockStatement(convertStatement(child, fields) + ConstantString("AS NUMERIC"))
              case _ =>
                ConstantString("CAST") +
                  blockStatement(convertStatement(child, fields) + "AS" + cast)
            }

          case _ => convertStatement(child, fields)
        }

      case ShiftLeft(child, position) =>
        blockStatement(convertStatement(child, fields) + ConstantString("<<") + convertStatement(position, fields))

      case ShiftRight(child, position) =>
        blockStatement(convertStatement(child, fields) + ConstantString(">>") + convertStatement(position, fields))

      case CaseWhen(branches, elseValue) =>
        ConstantString("CASE") +
          makeStatement(branches.map(whenClauseTuple =>
            ConstantString("WHEN") + convertStatement(whenClauseTuple._1, fields) + ConstantString("THEN") + convertStatement(whenClauseTuple._2, fields)
          ), "") +
          {
            elseValue match {
              case Some(value) =>
                ConstantString("ELSE") + convertStatement(value, fields)
              case None =>
                EmptyBigQuerySQLStatement()
            }
          } + ConstantString("END")

      case Coalesce(columns) =>
        ConstantString(expression.prettyName.toUpperCase) + blockStatement(makeStatement(columns.map(convertStatement(_, fields)), ", "))

      case If(predicate, trueValue, falseValue) =>
        ConstantString(expression.prettyName.toUpperCase) + blockStatement(convertStatements(fields, predicate, trueValue, falseValue))

      case InSet(child, hset) =>
        convertStatement( In(child, setToExpression(hset)), fields)

      case UnscaledValue(child) =>
        child.dataType match {
          case d: DecimalType =>
            blockStatement(convertStatement(child, fields) + "* POW( 10," + IntVariable(Some(d.scale)) + ")")
          case _ => null
        }

      case _: Cast =>
        convertCastExpression(expression, fields)

      case _: ScalarSubquery =>
        convertScalarSubqueryExpression(expression, fields)

      case _ => null
    })
  }

  def convertStringExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case _: Ascii | _: Concat | _: Length | _: Lower |
           _: StringLPad | _: StringRPad | _: StringTranslate |
           _: StringTrim | _: StringTrimLeft | _: StringTrimRight |
           _: Upper | _: StringInstr | _: InitCap |
           _: Substring =>
        ConstantString(expression.prettyName.toUpperCase()) + blockStatement(convertStatements(fields, expression.children: _*))
      case RegExpExtract(child, Literal(pattern: UTF8String, StringType), idx) =>
        ConstantString("REGEXP_EXTRACT") + blockStatement(convertStatement(child, fields) + "," + s"r'${pattern.toString}'" + "," + convertStatement(idx, fields))
     case _: RegExpReplace =>
        ConstantString("REGEXP_REPLACE") + blockStatement(convertStatement(expression.children.head, fields) + "," + s"r'${expression.children(1).toString}'" + "," + s"'${expression.children(2).toString}'")
      case _: FormatString | _: FormatNumber =>
        ConstantString("FORMAT") + blockStatement(convertStatements(fields, expression.children: _*))
      case _: Base64 =>
        ConstantString("TO_BASE64") + blockStatement(convertStatements(fields, expression.children: _*))
      case _: UnBase64 =>
        ConstantString("FROM_BASE64") + blockStatement(convertStatements(fields, expression.children: _*))
      case _: SoundEx =>
        ConstantString("UPPER") + blockStatement(ConstantString("SOUNDEX") + blockStatement(convertStatements(fields, expression.children: _*)))
      case _ => null
    })
  }

  def convertWindowExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case WindowExpression(windowFunction, windowSpec) =>
        windowFunction match {
          /**
           * Since we can't use a window frame clause with navigation functions and numbering functions,
           * we set the useWindowFrame to false
           */
          case _: Rank | _: DenseRank | _: PercentRank | _: RowNumber =>
            convertStatement(windowFunction, fields) +
            ConstantString("OVER") +
            convertWindowBlock(windowSpec, fields, generateWindowFrame = false)
          case _ =>
            convertStatement(windowFunction, fields) +
              ConstantString("OVER") +
              convertWindowBlock(windowSpec, fields, generateWindowFrame = true)
        }
      /**
       * Handling Numbering Functions here itself since they are a sub class of Window Expressions
       */
      case _: Rank | _: DenseRank | _: PercentRank | _: RowNumber =>
        ConstantString(expression.prettyName.toUpperCase) + ConstantString("()")
      case _ => null
    })
  }

  def convertWindowBlock(windowSpecDefinition: WindowSpecDefinition, fields: Seq[Attribute], generateWindowFrame: Boolean): BigQuerySQLStatement = {
    val partitionBy =
      if (windowSpecDefinition.partitionSpec.nonEmpty) {
        ConstantString("PARTITION BY") +
          makeStatement(windowSpecDefinition.partitionSpec.map(convertStatement(_, fields)), ",")
      } else {
        EmptyBigQuerySQLStatement()
      }

    val orderBy =
      if (windowSpecDefinition.orderSpec.nonEmpty) {
        ConstantString("ORDER BY") +
          makeStatement(windowSpecDefinition.orderSpec.map(convertStatement(_, fields)), ",")
      } else {
        EmptyBigQuerySQLStatement()
      }

    /**
     * Generating the window frame iff generateWindowFrame is true and the window spec has order spec in it
     */
    val windowFrame =
      if (generateWindowFrame && windowSpecDefinition.orderSpec.nonEmpty) {
        windowSpecDefinition.frameSpecification.sql
      } else {
        ""
      }

    blockStatement(partitionBy + orderBy + windowFrame)
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
      case _: DecimalType => "BIGDECIMAL"
      case IntegerType | ShortType | LongType => "INT64"
      case FloatType | DoubleType => "FLOAT64"
      case _ => null
    })

  final def performCastExpressionConversion(child: Expression, fields: Seq[Attribute], dataType: DataType): BigQuerySQLStatement =
    getCastType(dataType) match {
      case Some(cast) =>

        /**
         * For known unsupported data conversion, raise exception to break the pushdown process.
         * For example, BigQuery doesn't support to convert DATE/TIMESTAMP to NUMBER
         */
        (child.dataType, dataType) match {
          case (_: DateType | _: TimestampType,
          _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) => {
            throw new BigQueryPushdownUnsupportedException(
              "Pushdown failed due to unsupported conversion")
          }

          /**
           * BigQuery doesn't support casting from Integer to Bytes (https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#cast_as_bytes)
           * So handling this case separately.
           */
          case (_: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType ,_: ByteType) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + ConstantString("AS NUMERIC"))
          case _ =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
        }

      case _ => convertStatement(child, fields)
    }

  final def setToExpression(set: Set[Any]): Seq[Expression] = {
    set.map {
      case d: Decimal => Literal(d, DecimalType(d.precision, d.scale))
      case s @ (_: String | _: UTF8String) => Literal(s, StringType)
      case d: Double => Literal(d, DoubleType)
      case l: Long => Literal(l, LongType)
      case e: Expression => e
      case default =>
        throw new BigQueryPushdownUnsupportedException(
          "Pushdown unsupported for " + s"${default.getClass.getSimpleName} @ MiscStatement.setToExpression"
        )
    }.toSeq
  }

  // For supporting Scalar Subquery, we need specific implementations of BigQueryStrategy
  def convertScalarSubqueryExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertCheckOverflowExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertUnaryMinusExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertCastExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement
}
