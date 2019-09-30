package com.google.cloud.spark.bigquery

import java.math.BigInteger

import com.google.cloud.bigquery.{BigQuery, TableDefinition}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

// Used only to insert data into BigQuery. Getting the table metadata (existence,
// number of records, size, etc.) is done by API calls without reading existing
// data
case class BigQueryInsertableRelation(val bigQuery: BigQuery,
                                      val sqlContext: SQLContext,
                                      val options: SparkBigQueryOptions)
  extends BaseRelation
    with InsertableRelation
    with StrictLogging {


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logger.info(s"insert data=${data}, overwrite=$overwrite")
    // the helper also supports the v2 api
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    val helper = BigQueryWriteHelper(bigQuery, sqlContext, saveMode, options, data)
    helper.writeDataFrameToBigQuery
  }

  /**
    * Does this table exist?
    */
  def exists: Boolean = {
    // See https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/BigQuery.html#getTable-com.google.cloud.bigquery.TableId-com.google.cloud.bigquery.BigQuery.TableOption...-
    val table = getTable
    table.isDefined
  }

  /**
    * Is this table empty? A none-existing table is considered to be empty
    */
  def isEmpty: Boolean = numberOfRows.map(n => n.longValue() == 0).getOrElse(true)

  /**
    * Returns the number of rows in the table. If the table does not exist return None
    */
  def numberOfRows: Option[BigInteger] = getTable.map(t => t.getNumRows())

  private def getTable = Option(bigQuery.getTable(options.tableId))

  override def schema: StructType = {
    val tableInfo = bigQuery.getTable(options.tableId)
    val tableDefinition = tableInfo.getDefinition.asInstanceOf[TableDefinition]
    SchemaConverters.toSpark(tableDefinition.getSchema)
  }


}



