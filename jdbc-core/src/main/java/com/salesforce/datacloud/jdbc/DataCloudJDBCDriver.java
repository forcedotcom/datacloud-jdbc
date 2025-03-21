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

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Constants;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getBooleanOrDefault;

@Slf4j
public class DataCloudJDBCDriver implements Driver {
    private static Driver registeredDriver;

    static {
        try {
            register();
            log.info("DataCloud JDBC driver registered");
        } catch (SQLException e) {
            log.error("Error occurred while registering DataCloud JDBC driver. {}", e.getMessage());
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void register() throws SQLException {
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
        if (url == null) {
            throw new SQLException("Error occurred while registering JDBC driver. URL is null.");
        }

        if (!this.acceptsURL(url)) {
            return null;
        }

        val direct = getBooleanOrDefault(info, Constants.DIRECT, false);
        if (direct) {
            return directConnection(url, info);
        }

        return DataCloudConnection.of(url, info);
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

    private static DataCloudConnection directConnection(String url, Properties properties) throws SQLException {
        val skipAuth = getBooleanOrDefault(properties, Constants.DIRECT, false);
        if (!skipAuth) {
            throw new DataCloudJDBCException("Cannot establish direct connection without " + Constants.DIRECT + " enabled");
        }

        val connString = DataCloudConnectionString.of(url);
        val uri = URI.create(connString.getLoginUrl());

        log.info("Creating data cloud connection {}", uri);

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext();

        return DataCloudConnection.fromChannel(builder, properties);
    }
}
