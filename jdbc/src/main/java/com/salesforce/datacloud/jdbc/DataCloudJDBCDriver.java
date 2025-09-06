/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;
import static com.salesforce.datacloud.jdbc.util.Constants.LOGIN_URL;
import static com.salesforce.datacloud.jdbc.util.Constants.USER;
import static com.salesforce.datacloud.jdbc.util.Constants.USER_NAME;

import com.salesforce.datacloud.jdbc.auth.AuthenticationSettings;
import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProcessor;
import com.salesforce.datacloud.jdbc.config.DriverVersion;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.interceptor.TokenProcessorSupplier;
import com.salesforce.datacloud.jdbc.interceptor.TracingHeadersInterceptor;
import com.salesforce.datacloud.jdbc.soql.DataspaceClient;
import com.salesforce.datacloud.jdbc.util.DirectDataCloudConnection;
import com.salesforce.datacloud.jdbc.util.PropertyValidator;
import io.grpc.ManagedChannelBuilder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class DataCloudJDBCDriver implements Driver {
    private static Driver registeredDriver;

    static {
        try {
            log.info(
                    "Registering DataCloud JDBC driver. info={}, classLoader={}",
                    formatDriverInfo(),
                    DataCloudJDBCDriver.class.getClassLoader());
            register();
            log.info("DataCloud JDBC driver registered");

        } catch (SQLException e) {
            log.error("Error occurred while registering DataCloud JDBC driver. {}", e.getMessage());
            throw new ExceptionInInitializerError(e);
        }
    }

    private static synchronized void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException("Driver is already registered. It can only be registered once.");
        }
        registeredDriver = new DataCloudJDBCDriver();
        DriverManager.registerDriver(registeredDriver);
    }

    public static boolean isRegistered() {
        return registeredDriver != null;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        log.info("connect url={}", url);

        if (url == null) {
            throw new SQLException("Error occurred while registering JDBC driver. URL is null.");
        }

        if (!acceptsURL(url)) {
            return null;
        }

        try {
            // Validate properties to raise user errors on unknown keys
            PropertyValidator.validate(info);
            if (DirectDataCloudConnection.isDirect(info)) {
                log.info("Using direct connection");
                return DirectDataCloudConnection.of(url, info);
            }

            log.info("Using OAuth-based connection");
            return oauthBasedConnection(url, info);
        } catch (Exception e) {
            log.error("Failed to connect with URL {}: {}", url, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return DataCloudConnectionString.acceptsUrl(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return DriverVersion.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return DriverVersion.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return null;
    }

    private static DataCloudTokenProcessor getDataCloudTokenProcessor(Properties properties)
            throws DataCloudJDBCException {
        if (!AuthenticationSettings.hasAny(properties)) {
            throw new DataCloudJDBCException("No authentication settings provided");
        }

        return DataCloudTokenProcessor.of(properties);
    }

    static DataCloudConnection oauthBasedConnection(String url, Properties properties) throws SQLException {
        val connectionString = DataCloudConnectionString.of(url);
        addClientUsernameIfRequired(properties);
        connectionString.withParameters(properties);
        properties.setProperty(LOGIN_URL, connectionString.getLoginUrl());

        val tokenProcessor = getDataCloudTokenProcessor(properties);
        val authInterceptor = TokenProcessorSupplier.of(tokenProcessor);

        val host = tokenProcessor.getDataCloudToken().getTenantUrl();
        final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(
                        host, DataCloudConnection.DEFAULT_PORT)
                .intercept(TracingHeadersInterceptor.of());

        val dataspaceClient = new DataspaceClient(properties, tokenProcessor);

        return DataCloudConnection.of(
                builder, properties, authInterceptor, tokenProcessor::getLakehouse, dataspaceClient, connectionString);
    }

    static void addClientUsernameIfRequired(Properties properties) {
        if (properties.containsKey(USER)) {
            properties.computeIfAbsent(USER_NAME, p -> properties.get(USER));
        }
    }
}
