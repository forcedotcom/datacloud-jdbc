package com.salesforce.datacloud.spark

import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import scala.util.Using

/** A Spark Datasource for reading a Hyper result.
  *
  * Given a query id, this class can be used with Spark's data source API:
  *
  * ```
  * spark.read
  *   .format("com.salesforce.datacloud.spark.HyperResultSource")
  *   .option("jdbcUrl", s"jdbc:salesforce-hyper://...")
  *   .option("queryId", queryId)
  *   .load()
  * ```
  *
  * The `queryId` option indicates a query id acquired, e.g., via
  * `DataCloudStatement.getQueryId()`. All other options are identical to the
  * JDBC connection options.
  */
class HyperResultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val parsedOptions =
      HyperResultSourceOptions.fromOptions(options.asCaseSensitiveMap())

    Using.resource(parsedOptions.createConnection()) { conn =>
      TypeMapping.getSparkFields(
        conn.getSchemaForQueryId(parsedOptions.queryId)
      )
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]
  ): Table = {
    val parsedOptions =
      HyperResultSourceOptions.fromOptions(properties)
    HyperResultTable(parsedOptions, schema)
  }
}
