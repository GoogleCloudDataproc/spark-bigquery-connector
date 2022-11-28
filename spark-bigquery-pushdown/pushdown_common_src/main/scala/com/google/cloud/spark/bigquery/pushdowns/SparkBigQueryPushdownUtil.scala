package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.{DEFAULT_FALLBACK, getOptionFromMultipleParams}
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.common.collect.ImmutableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, EmptyRow, ExprId, Expression, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LogicalPlan, Project, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
 * Static methods for query pushdown functionality
 */
object SparkBigQueryPushdownUtil {
  def enableBigQueryStrategy(session: SparkSession, bigQueryStrategy: BigQueryStrategy): Unit = {
    if (!session.experimental.extraStrategies.exists(
      s => s.isInstanceOf[BigQueryStrategy]
    )) {
      session.experimental.extraStrategies ++= Seq(bigQueryStrategy)
    }
  }

  def disableBigQueryStrategy(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[BigQueryStrategy])
  }

  /**
   * Creates a new BigQuerySQLStatement by adding parenthesis around the passed in statement and adding an alias for it
   * @param stmt
   * @param alias
   * @return
   */
  def blockStatement(stmt: BigQuerySQLStatement, alias: String): BigQuerySQLStatement =
    blockStatement(stmt) + "AS" + ConstantString(alias.toUpperCase).toStatement

  def blockStatement(stmt: BigQuerySQLStatement): BigQuerySQLStatement =
    ConstantString("(") + stmt + ")"

  /**
   * Creates a new BigQuerySQLStatement by taking a list of BigQuerySQLStatement and folding it into one BigQuerySQLStatement separated by a delimiter
   * @param seq
   * @param delimiter
   * @return
   */
  def makeStatement(seq: Seq[BigQuerySQLStatement], delimiter: String): BigQuerySQLStatement =
    makeStatement(seq, ConstantString(delimiter).toStatement)

  def makeStatement(seq: Seq[BigQuerySQLStatement], delimiter: BigQuerySQLStatement): BigQuerySQLStatement =
    seq.foldLeft(EmptyBigQuerySQLStatement()) {
      case (left, stmt) =>
        if (left.isEmpty) stmt else left + delimiter + stmt
    }

  /** This adds an attribute as part of a SQL expression, searching in the provided
   * fields for a match, so the subquery qualifiers are correct.
   *
   * @param attr   The Spark Attribute object to be added.
   * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
   *               usually derived from the output of a subquery.
   * @return A BigQuerySQLStatement representing the attribute expression.
   */
  def addAttributeStatement(attr: Attribute, fields: Seq[Attribute]): BigQuerySQLStatement =
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttributeStatement(resolved.qualifier, resolved.name)
      case None => qualifiedAttributeStatement(attr.qualifier, attr.name)
    }

  def qualifiedAttributeStatement(alias: Seq[String], name: String): BigQuerySQLStatement =
    ConstantString(qualifiedAttribute(alias, name)).toStatement

  def qualifiedAttribute(alias: Seq[String], name: String): String = {
    val str =
      if (alias.isEmpty) ""
      else alias.map(a => a.toUpperCase).mkString(".") + "."

    str + name.toUpperCase
  }

  /**
   * Rename projections so as to have unique column names across subqueries
   * @param origOutput
   * @param alias
   * @return
   */
  def renameColumns(origOutput: Seq[NamedExpression], alias: String, expressionFactory: SparkExpressionFactory): Seq[NamedExpression] = {
    val col_names = Iterator.from(0).map(n => s"COL_$n")

    origOutput.map { expr =>
      val metadata = expr.metadata

      val altName = s"""${alias}_${col_names.next()}"""

      expr match {
        case a@Alias(child: Expression, name: String) =>
          expressionFactory.createAlias(child, altName, a.exprId, Seq.empty[String], Some(metadata))
        case _ =>
          expressionFactory.createAlias(expr, altName, expr.exprId, Seq.empty[String], Some(metadata))
      }
    }
  }

  /**
   * Method to convert Expression into NamedExpression.
   * If the Expression is not of type NamedExpression, we create an Alias from the expression and attribute
   */
  def convertExpressionToNamedExpression(projections: Seq[Expression],
                                         output: Seq[Attribute],
                                         expressionFactory: SparkExpressionFactory): Seq[NamedExpression] = {
    projections zip output map { expression =>
      expression._1 match {
        case expr: NamedExpression => expr
        case _ => expressionFactory.createAlias(expression._1, expression._2.name, expression._2.exprId, Seq.empty[String], Some(expression._2.metadata))
      }
    }
  }

  def doExecuteSparkPlan(output: Seq[Attribute], rdd: RDD[InternalRow]): RDD[InternalRow] = {
    val schema = StructType(
      output.map(attr => StructField(attr.name, attr.dataType, attr.nullable))
    )

    rdd.mapPartitions { iter =>
      val project = UnsafeProjection.create(schema)
      iter.map(project)
    }
  }

  /**
   * Returns a copy of the LogicalPlan after removing the passed-in Project node
   * from the plan.
   */
  def removeProjectNodeFromPlan(plan: LogicalPlan, nodeToRemove: Project): LogicalPlan = {
    plan.transform({
      case node@Project(_, child) if node.fastEquals(nodeToRemove) => child
    })
  }

  /**
   * remove Cast from the projectList of the passed in Project node
   */
  def removeCastFromProjectList(node: Project, expressionFactory: SparkExpressionFactory): Project = {
    var projectList: Seq[NamedExpression] = Seq()
    node.projectList.foreach{
      case Alias(child, name) =>
        child match {
          case CastExpressionExtractor(expression) => projectList = projectList :+ expressionFactory.createAlias(expression.asInstanceOf[Cast].child, name, NamedExpression.newExprId, Seq.empty, None)
          case expression => projectList = projectList :+ expressionFactory.createAlias(expression, name, NamedExpression.newExprId, Seq.empty, None)
        }
      case node => projectList = projectList :+ node
    }
    Project(projectList, node.child)
  }

  /**
   * Create a ProjectList with only Cast's of the passed in node ProjectList
   */
  def createProjectListWithCastToString(node: Project, expressionFactory: SparkExpressionFactory): Seq[NamedExpression] = {
    var projectList: Seq[NamedExpression] = Seq()
    node.projectList.foreach(
      child =>
        projectList = projectList :+ expressionFactory.createAlias(Cast.apply(child.toAttribute, StringType), child.name, NamedExpression.newExprId, Seq.empty, None)
    )
    projectList
  }

  /**
   * Returns a copy of the updated LogicalPlan by adding a project plan to the passed in node.
   * Takes in the LogicalPlan and the Project Plan to update.
   * Remove the cast, if there is any from the passed project plan
   * and adds another Project Plan on top of the passed node to update
   */
  def addProjectNodeToThePlan(plan: LogicalPlan, nodeToUpdate: Project, expressionFactory: SparkExpressionFactory): LogicalPlan = {
    plan.transform({
      case node@Project(_, _) if node.fastEquals(nodeToUpdate) =>
        val projectWithoutCast = removeCastFromProjectList(nodeToUpdate, expressionFactory)
        val projectListWithCast = createProjectListWithCastToString(projectWithoutCast, expressionFactory)
        Project(projectListWithCast, projectWithoutCast)
    })
  }

  def isLimitTheChildToProjectNode(node: Project): Boolean = {
    if (node.child.isInstanceOf[GlobalLimit]) true else false
  }
}
