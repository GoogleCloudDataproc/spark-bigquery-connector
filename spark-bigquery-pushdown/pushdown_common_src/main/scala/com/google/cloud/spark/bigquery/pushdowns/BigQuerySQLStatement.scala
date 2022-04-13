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

/**
 * SQL string wrapper class
 */
class BigQuerySQLStatement(val list: List[StatementElement] = Nil) {

  /**
   * Create a new BigQuerySQLStatement by adding a StatementElement to the current BigQuerySQLStatement
   * @param element
   * @return
   */
  def +(element: StatementElement): BigQuerySQLStatement =
    new BigQuerySQLStatement(element :: list)

  /**
   * Create a new BigQuerySQLStatement by adding a BigQuerySQLStatement to the current BigQuerySQLStatement
   * @param element
   * @return
   */
  def +(statement: BigQuerySQLStatement): BigQuerySQLStatement =
    new BigQuerySQLStatement(statement.list ::: list)

  /**
   * Create a new BigQuerySQLStatement by adding a String to the current BigQuerySQLStatement
   *
   * @param element
   * @return
   */
  def +(str: String): BigQuerySQLStatement = this + ConstantString(str)

  def isEmpty: Boolean = list.isEmpty

  /**
   * Converts a BigQuerySQLStatement to SQL runnable by BigQuery
   * @return
   */
  override def toString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x)
      case x: VariableElement[_] =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x.sql)
    }

    buffer.toString()
  }
}

object EmptyBigQuerySQLStatement {
  def apply(): BigQuerySQLStatement = new BigQuerySQLStatement()
}

/**
 * Trait for wrapping various types like String, Long, Float etc
 */
sealed trait StatementElement {

  val value: String

  /**
   * Create a new BigQuerySQLStatement by adding a StatementElement to the current StatementElement
   *
   * @param element
   * @return
   */
  def +(element: StatementElement): BigQuerySQLStatement =
    new BigQuerySQLStatement(element :: List[StatementElement](this))

  /**
   * Create a new BigQuerySQLStatement by adding a BigQuerySQLStatement to the current StatementElement
   *
   * @param element
   * @return
   */
  def +(statement: BigQuerySQLStatement): BigQuerySQLStatement =
    new BigQuerySQLStatement(
      statement.list ::: List[StatementElement](this)
    )

  /**
   * Create a new BigQuerySQLStatement by adding a String to the current StatementElement
   *
   * @param element
   * @return
   */
  def +(str: String): BigQuerySQLStatement = this + ConstantString(str)

  override def toString: String = value

  def toStatement: BigQuerySQLStatement =
    new BigQuerySQLStatement(List[StatementElement](this))

  def sql: String = value
}

case class ConstantString(override val value: String) extends StatementElement

sealed trait VariableElement[T] extends StatementElement {
  override val value = "?"

  val variable: Option[T]

  override def sql: String = if (variable.isDefined) variable.get.toString else "NULL"

}

case class StringVariable(override val variable: Option[String])
  extends VariableElement[String] {
  override def sql: String = if (variable.isDefined) s"""'${variable.get}'""" else "NULL"
}

case class IntVariable(override val variable: Option[Int])
  extends VariableElement[Int]

case class LongVariable(override val variable: Option[Long])
  extends VariableElement[Long]

case class ShortVariable(override val variable: Option[Short])
  extends VariableElement[Short]

case class FloatVariable(override val variable: Option[Float])
  extends VariableElement[Float]

case class DoubleVariable(override val variable: Option[Double])
  extends VariableElement[Double]

case class BooleanVariable(override val variable: Option[Boolean])
  extends VariableElement[Boolean]

case class ByteVariable(override val variable: Option[Byte])
  extends VariableElement[Byte]

object ConstantStringVal {
  def apply(l: Any): StatementElement = {
    if (l == null || l.toString.toLowerCase == "null") {
      ConstantString("NULL")
    } else {
      ConstantString(l.toString)
    }
  }
}
