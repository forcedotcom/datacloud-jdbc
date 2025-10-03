/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptional;

import com.salesforce.datacloud.jdbc.core.ConnectionProperties;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.GrpcChannelProperties;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.JdbcURL;
import com.salesforce.datacloud.jdbc.util.PropertyParsingUtils;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import io.grpc.ManagedChannelBuilder;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * The DataCloudDatasource provides a type-safe way to create a DataCloudConnection.
 *
 * Use the `DataCloudDatasource.builder()` interface to configure the data source.
 * After configuring the data source, call `DataCloudDatasource.getConnection()` to connect.
 *
 * Furthermore, `connectUsingProperties` can be used to connect using a URL and properties.
 */
@Slf4j
@Builder
public class HyperDatasource implements DataSource {
    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY =
            "Datasource method is not supported in Data Cloud query";

    private final String host;

    @Builder.Default
    private final int port = -1;

    private final ConnectionProperties connectionProperties;
    private final GrpcChannelProperties grpcChannelProperties;
    String dataspace;

    @Override
    public Connection getConnection() throws SQLException {
        return createConnection(host, port, connectionProperties, grpcChannelProperties, dataspace, /*jdbcUrl=*/ null);
    }

    /**
     * Checks if the URL is a valid Data Cloud JDBC URL.
     */
    public static boolean acceptsURL(String url) {
        // We only check for the prefix. We don't check if, e.g., all parameters
        // are valid. The `connect` method will throw an exception if the
        // parameters are invalid. This is consistent with the behavior expected
        // by the JDBC specification.
        return url.startsWith("jdbc:salesforce-hyper:");
    }

    /**
     * Connects to the Data Cloud using the given URL and properties.
     * Similar to the interface accessible via `DriverManager.getConnection()`.
     */
    public static DataCloudConnection connectUsingProperties(@NonNull String url, Properties info) throws SQLException {
        log.info("connect url={}", url);

        if (!acceptsURL(url)) {
            throw new DataCloudJDBCException("Invalid URL. URL must start with 'jdbc:salesforce-hyper:'");
        }

        try {
            // Parse the URL and parameters
            final JdbcURL jdbcUrl = JdbcURL.of(url);
            String host = jdbcUrl.getHost();
            int port = jdbcUrl.getPort();
            val properties = info != null ? (Properties) info.clone() : new Properties();
            jdbcUrl.addParametersToProperties(properties);
            val connectionProperties = ConnectionProperties.ofDestructive(properties);
            val grpcChannelProperties = GrpcChannelProperties.ofDestructive(properties);
            String dataspace = takeOptional(properties, "dataspace").orElse("");
            PropertyParsingUtils.validateRemainingProperties(properties);

            // Setup the connection
            return createConnection(host, port, connectionProperties, grpcChannelProperties, dataspace, jdbcUrl);
        } catch (SQLException e) {
            log.error("Failed to connect with URL {}: {}", url, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Internal utility function to create a DataCloudConnection with the given properties.
     *
     * The jdbcUrl is optional and will only influence `DatabaseMetaData.getURL()`.
     * The actual connection will be created with the properties provided.
     */
    private static DataCloudConnection createConnection(
            @NonNull String host,
            int port,
            @NonNull ConnectionProperties connectionProperties,
            @NonNull GrpcChannelProperties grpcChannelProperties,
            @NonNull String dataspace,
            JdbcURL jdbcUrl)
            throws SQLException {
        port = port == -1 ? 7483 : port;
        ManagedChannelBuilder<?> builder =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        JdbcDriverStubProvider stubProvider = JdbcDriverStubProvider.of(builder, grpcChannelProperties);
        return DataCloudConnection.of(stubProvider, connectionProperties, dataspace, jdbcUrl);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @SneakyThrows
    @Override
    public Logger getParentLogger() {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
