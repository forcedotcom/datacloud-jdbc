/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc;

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
                    DriverVersion.formatDriverInfo(),
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
