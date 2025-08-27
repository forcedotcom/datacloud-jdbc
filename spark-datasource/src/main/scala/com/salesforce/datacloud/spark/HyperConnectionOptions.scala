package com.salesforce.datacloud.spark

import java.util.Properties
import com.salesforce.datacloud.jdbc.core.DataCloudConnection
import com.salesforce.datacloud.jdbc.util.DirectDataCloudConnection

private case class HyperConnectionOptions(
    host: String,
    port: Int,
    // SSL/TLS Configuration Options (TLS will be detected inside DirectDataCloudConnection by default)
    truststorePath: Option[String] = None,
    truststorePassword: Option[String] = None,
    truststoreType: Option[String] = None,
    clientCertPath: Option[String] = None,
    clientKeyPath: Option[String] = None,
    caCertPath: Option[String] = None,
    // Authentication options
    tenantId: Option[String] = None,
    // Internal SSL disable flag (for testing)
    sslDisabled: Option[Boolean] = None
) {
  def createConnection(): DataCloudConnection = {
    // Use DirectDataCloudConnection for SSL auto-detection
    createDirectConnection()
  }

  private def createDirectConnection(): DataCloudConnection = {
    // Build connection URL - DirectDataCloudConnection expects format: jdbc:salesforce-datacloud://host:port
    val url = s"jdbc:salesforce-datacloud://$host:$port"

    // Build properties for connection
    val properties = new Properties()

    // Set direct connection mode
    properties.setProperty("direct", "true")

    // SSL configuration properties (DirectDataCloudConnection will auto-detect)
    truststorePath.foreach(properties.setProperty("truststore_path", _))
    truststorePassword.foreach(properties.setProperty("truststore_password", _))
    truststoreType.foreach(properties.setProperty("truststore_type", _))
    clientCertPath.foreach(properties.setProperty("client_cert_path", _))
    clientKeyPath.foreach(properties.setProperty("client_key_path", _))
    caCertPath.foreach(properties.setProperty("ca_cert_path", _))

    // ssl_disabled flag, this can change in future
    sslDisabled.foreach(disabled =>
      properties.setProperty("ssl_disabled", disabled.toString)
    )

    // Add authentication properties
    tenantId.foreach(properties.setProperty("tenantId", _))

    // Use DirectDataCloudConnection for SSL auto-detection
    DirectDataCloudConnection.of(url, properties)
  }
}

private object HyperConnectionOptions {
  def fromOptions(
      options: java.util.Map[String, String]
  ): HyperConnectionOptions = {

    // Helper function to safely get optional values
    def getOption(key: String): Option[String] =
      Option(options.get(key)).filter(_.nonEmpty)

    // Required options with defaults
    val host = getOption("host").getOrElse("127.0.0.1")
    val port =
      getOption("port")
        .map(_.toInt)
        .getOrElse(443) // HTTPS port should be the default port

    HyperConnectionOptions(
      host = host,
      port = port,
      // SSL Configuration (auto-detected by DirectDataCloudConnection)
      truststorePath = getOption("truststore_path"),
      truststorePassword = getOption("truststore_password"),
      truststoreType = getOption("truststore_type"),
      clientCertPath = getOption("client_cert_path"),
      clientKeyPath = getOption("client_key_path"),
      caCertPath = getOption("ca_cert_path"),
      // Authentication options
      tenantId = getOption("tenant_id"),
      // SSL disable flag for internal use case, the implementation may change in future
      sslDisabled = getOption("ssl_disabled").map(_.toBoolean)
    )
  }
}
