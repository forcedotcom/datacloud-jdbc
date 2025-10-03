package com.salesforce.datacloud.spark

import java.sql.DriverManager
import java.util.Properties
import com.salesforce.datacloud.jdbc.core.DataCloudConnection
import io.grpc.ManagedChannelBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private case class HyperResultSourceOptions(
    jdbcUrl: String,
    properties: Properties,
    queryId: String
) {
  def createConnection(): DataCloudConnection = {
    DriverManager
      .getConnection(jdbcUrl, properties)
      .asInstanceOf[DataCloudConnection]
  }
}

private object HyperResultSourceOptions {
  def fromOptions(
      options: java.util.Map[String, String]
  ): HyperResultSourceOptions = {
    val props = new Properties()
    options.forEach((key, value) => props.setProperty(key, value))

    val jdbcUrl = props.getProperty("jdbcUrl");
    if (jdbcUrl == null) {
      throw new IllegalArgumentException(
        s"Missing `jdbcUrl` property. Available keys: ${options.keySet().toArray.mkString(", ")}"
      )
    }
    props.remove("jdbcUrl")
    val queryId = props.getProperty("queryId")
    if (queryId == null) {
      throw new IllegalArgumentException(
        s"Missing `queryId` property"
      )
    }
    props.remove("queryId")

    HyperResultSourceOptions(jdbcUrl, props, queryId)
  }
}
