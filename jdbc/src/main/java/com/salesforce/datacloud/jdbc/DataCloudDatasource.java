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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataCloudDatasource implements DataSource {
    private static final String USERNAME_PROPERTY = "userName";
    private static final String PASSWORD_PROPERTY = "password";
    private static final String PRIVATE_KEY_PROPERTY = "privateKey";
    private static final String REFRESH_TOKEN_PROPERTY = "refreshToken";
    private static final String CORE_TOKEN_PROPERTY = "coreToken";
    private static final String CLIENT_ID_PROPERTY = "clientId";
    private static final String CLIENT_SECRET_PROPERTY = "clientSecret";
    private static final String INTERNAL_ENDPOINT_PROPERTY = "internalEndpoint";
    private static final String PORT_PROPERTY = "port";
    private static final String TENANT_ID_PROPERTY = "tenantId";
    private static final String DATASPACE_PROPERTY = "dataspace";
    private static final String CORE_TENANT_ID_PROPERTY = "coreTenantId";

    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY =
            "Datasource method is not supported in Data Cloud query";

    private String connectionUrl;
    private final Properties properties = new Properties();

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return DriverManager.getConnection(getConnectionUrl(), properties);
        } catch (SQLException e) {
            throw new DataCloudJDBCException(e);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        setUserName(username);
        setPassword(password);
        return getConnection();
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

    private String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public void setUserName(String userName) {
        this.properties.setProperty(USERNAME_PROPERTY, userName);
    }

    public void setPassword(String password) {
        this.properties.setProperty(PASSWORD_PROPERTY, password);
    }

    public void setPrivateKey(String privateKey) {
        this.properties.setProperty(PRIVATE_KEY_PROPERTY, privateKey);
    }

    public void setRefreshToken(String refreshToken) {
        this.properties.setProperty(REFRESH_TOKEN_PROPERTY, refreshToken);
    }

    public void setCoreToken(String coreToken) {
        this.properties.setProperty(CORE_TOKEN_PROPERTY, coreToken);
    }

    public void setInternalEndpoint(String internalEndpoint) {
        this.properties.setProperty(INTERNAL_ENDPOINT_PROPERTY, internalEndpoint);
    }

    public void setPort(String port) {
        this.properties.setProperty(PORT_PROPERTY, port);
    }

    public void setTenantId(String tenantId) {
        this.properties.setProperty(TENANT_ID_PROPERTY, tenantId);
    }

    public void setDataspace(String dataspace) {
        this.properties.setProperty(DATASPACE_PROPERTY, dataspace);
    }

    public void setCoreTenantId(String coreTenantId) {
        this.properties.setProperty(CORE_TENANT_ID_PROPERTY, coreTenantId);
    }

    public void setClientId(String clientId) {
        this.properties.setProperty(CLIENT_ID_PROPERTY, clientId);
    }

    public void setClientSecret(String clientSecret) {
        this.properties.setProperty(CLIENT_SECRET_PROPERTY, clientSecret);
    }
}
