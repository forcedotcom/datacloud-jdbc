package com.salesforce.datacloud.spark

import java.util.Properties
import com.salesforce.datacloud.jdbc.core.DataCloudConnection
import io.grpc.ManagedChannelBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private case class HyperConnectionOptions(
    host: String,
    port: Int
) {
  def createConnection(): DataCloudConnection = {
    val channel = ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext();
    val properties = new Properties();

    DataCloudConnection.of(channel, properties)
  }
}

private object HyperConnectionOptions {
  def fromOptions(
      options: java.util.Map[String, String]
  ): HyperConnectionOptions = {
    var host = options.get("host")
    if (host == null) {
      host = "127.0.0.1"
    }
    HyperConnectionOptions(host, options.get("port").toInt)
  }
}
