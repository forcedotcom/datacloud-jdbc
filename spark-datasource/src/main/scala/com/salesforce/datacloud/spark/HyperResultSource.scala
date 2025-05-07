package com.salesforce.datacloud.spark

import com.salesforce.datacloud.jdbc.core.DataCloudConnection
import java.util.Properties
import io.grpc.ManagedChannelBuilder
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import java.sql.ResultSetMetaData
import org.apache.spark.sql.types.StructField
import com.salesforce.datacloud.spark.TypeMapping.getSparkType

/** A Spark Datasource for reading a Hyper result.
  *
  * Given a query id, this class can be used with Spark's data source API:
  *
  * ```
  * spark.read
  *   .format("com.salesforce.datacloud.spark.HyperResultSource")
  *   .option("query_id", queryId)
  *   .option("port", hyperServerProcess.getPort())
  *   .load()
  * ```
  *
  * The `query_id` option indicates a query id acquired, e.g., via
  * `DataCloudStatement.getQueryId()`. All other options are identical to the
  * JDBC connection options.
  *
  * TODO: actually forward the options to the JDBC connection.
  */
class HyperResultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val queryId = options.get("query_id")
    if (queryId == null) {
      throw new IllegalArgumentException(
        s"Missing `query_id` property"
      )
    }
    val connectionOptions =
      HyperConnectionOptions.fromOptions(options.asCaseSensitiveMap())

    val conn = connectionOptions.createConnection()
    // TODO XXX: use `getResultSet` instead of `getChunkBasedResultSet`, as soon as it was added to the JDBC driver
    val rs = conn.getChunkBasedResultSet(queryId, 0, 0);
    TypeMapping.getSparkFields(rs.getMetaData())
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]
  ): Table = {
    val queryId = properties.get("query_id")
    if (queryId == null) {
      throw new IllegalArgumentException(
        s"Missing `query_id` property"
      )
    }
    val connectionOptions = HyperConnectionOptions.fromOptions(properties)
    HyperResultTable(connectionOptions, queryId, schema)
  }
}
