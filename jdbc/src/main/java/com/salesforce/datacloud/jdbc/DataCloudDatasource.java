/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProvider;
import com.salesforce.datacloud.jdbc.auth.SalesforceAuthProperties;
import com.salesforce.datacloud.jdbc.core.ConnectionProperties;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.GrpcChannelProperties;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.http.HttpClientProperties;
import com.salesforce.datacloud.jdbc.interceptor.AuthorizationHeaderInterceptor;
import com.salesforce.datacloud.jdbc.interceptor.TokenProcessorSupplier;
import com.salesforce.datacloud.jdbc.interceptor.TracingHeadersInterceptor;
import com.salesforce.datacloud.jdbc.soql.DataspaceClient;
import com.salesforce.datacloud.jdbc.util.JdbcURL;
import com.salesforce.datacloud.jdbc.util.PropertyParsingUtils;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import io.grpc.ManagedChannelBuilder;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
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
 * The DataCloudDatasource provides a type-safe way create a DataCloudConnection.
 *
 * Use the `DataCloudDatasource.builder()` interface to configure the data source.
 * After configuring the data source, call `DataCloudDatasource.getConnection()` to connect.
 *
 * Furthermore, `connectUsingProperties` can be used to connect using a URL and properties.
 */
@Slf4j
@Builder
public class DataCloudDatasource implements DataSource {
    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY =
            "Datasource method is not supported in Data Cloud query";

    private final ConnectionProperties connectionProperties;
    private final GrpcChannelProperties grpcChannelProperties;
    private final HttpClientProperties httpClientProperties;
    private final SalesforceAuthProperties authProperties;

    @Override
    public Connection getConnection() throws SQLException {
        return createConnection(
                connectionProperties, grpcChannelProperties, httpClientProperties, authProperties, null);
    }

    /**
     * Checks if the URL is a valid Data Cloud JDBC URL.
     */
    public static boolean acceptsURL(String url) {
        // We only check for the prefix. We don't check if, e.g., all parameters
        // are valid. The `connect` method will throw an exception if the
        // parameters are invalid. This is consistent with the behavior expected
        // by the JDBC specification.
        return url.startsWith("jdbc:salesforce-datacloud:");
    }

    /**
     * Connects to the Data Cloud using the given URL and properties.
     * Similar to the interface accessible via `DriverManager.getConnection()`.
     */
    public static DataCloudConnection connectUsingProperties(@NonNull String url, Properties info) throws SQLException {
        log.info("connect url={}", url);

        if (!acceptsURL(url)) {
            throw new SQLException("Invalid URL. URL must start with 'jdbc:salesforce-datacloud:'");
        }

        try {
            // Determine the login URL
            final JdbcURL jdbcUrl = JdbcURL.of(url);
            if (jdbcUrl.getHost().startsWith("http://") || jdbcUrl.getHost().startsWith("https://")) {
                throw new SQLException("The JDBC URL must not contain a http/https prefix");
            }
            int effectivePort = jdbcUrl.getPort() == -1 ? 443 : jdbcUrl.getPort();
            URI loginUrl;
            try {
                loginUrl = new URI("https://" + jdbcUrl.getHost() + ":" + String.valueOf(effectivePort));
            } catch (URISyntaxException e) {
                throw new SQLException("Invalid URI syntax: " + e.getReason());
            }

            // Parse the properties
            val properties = info != null ? (Properties) info.clone() : new Properties();
            jdbcUrl.addParametersToProperties(properties);
            normalizeUsernameProperty(properties);
            val connectionProperties = ConnectionProperties.ofDestructive(properties);
            val grpcChannelProperties = GrpcChannelProperties.ofDestructive(properties);
            val httpClientProperties = HttpClientProperties.ofDestructive(properties);
            val authProperties = SalesforceAuthProperties.ofDestructive(loginUrl, properties);
            PropertyParsingUtils.validateRemainingProperties(properties);

            return createConnection(
                    connectionProperties, grpcChannelProperties, httpClientProperties, authProperties, jdbcUrl);
        } catch (SQLException e) {
            log.error("Failed to connect with URL {}: {}", url, e.getMessage(), e);
            throw e;
        }
    }

    private static void normalizeUsernameProperty(Properties properties) throws SQLException {
        // For backwards compatibility, we support both the "user" and "userName" properties.
        // Normalize it to "userName".
        if (properties.containsKey("user")) {
            if (properties.containsKey("userName")) {
                throw new SQLException("userName and user properties cannot be mixed", "28000");
            }
            properties.setProperty("userName", properties.getProperty("user"));
            properties.remove("user");
        }
    }

    /**
     * Internal utility function to create a DataCloudConnection with the given properties.
     *
     * The jdbcUrl is optional and will only influence `DatabaseMetaData.getURL()`.
     * The actual connection will be created with the properties provided.
     */
    private static DataCloudConnection createConnection(
            @NonNull ConnectionProperties connectionProperties,
            @NonNull GrpcChannelProperties grpcChannelProperties,
            @NonNull HttpClientProperties httpClientProperties,
            @NonNull SalesforceAuthProperties authProperties,
            JdbcURL jdbcUrl)
            throws SQLException {
        // Setup the authInterceptor
        val tokenProvider = DataCloudTokenProvider.of(httpClientProperties, authProperties);
        val tokenSupplier = new TokenProcessorSupplier(tokenProvider);
        val authInterceptor = AuthorizationHeaderInterceptor.of(tokenSupplier);

        // Setup the dataspace client
        val dataspaceClient = new DataspaceClient(httpClientProperties, tokenProvider);

        // Setup the gRPC stub provider
        val host = tokenProvider.getDataCloudToken().getTenantUrl();
        final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, 443)
                .intercept(authInterceptor)
                .intercept(TracingHeadersInterceptor.of());
        val stubProvider = JdbcDriverStubProvider.of(builder, grpcChannelProperties);

        return DataCloudConnection.of(
                stubProvider,
                connectionProperties,
                jdbcUrl,
                authProperties.getUserName(),
                authProperties.getDataspace(),
                tokenProvider::getLakehouseName,
                dataspaceClient);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @SneakyThrows
    @Override
    public Logger getParentLogger() {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
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
