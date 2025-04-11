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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class DataCloudDatasourceTest {
    private static final DataCloudDatasource dataCloudDatasource = new DataCloudDatasource();

    @Test
    void testGetConnectionReturnsInstanceOfConnection() throws SQLException {
        val expectedProperties = new Properties();
        val connectionUrl = UUID.randomUUID().toString();
        val userName = UUID.randomUUID().toString();
        val password = UUID.randomUUID().toString();
        val privateKey = UUID.randomUUID().toString();
        val coreToken = UUID.randomUUID().toString();
        val refreshToken = UUID.randomUUID().toString();
        val clientId = UUID.randomUUID().toString();
        val clientSecret = UUID.randomUUID().toString();
        val internalEndpoint = UUID.randomUUID().toString();
        val port = UUID.randomUUID().toString();
        val tenantId = UUID.randomUUID().toString();
        val dataspace = UUID.randomUUID().toString();
        val coreTenantId = UUID.randomUUID().toString();

        expectedProperties.setProperty("userName", userName);
        expectedProperties.setProperty("password", password);
        expectedProperties.setProperty("privateKey", privateKey);
        expectedProperties.setProperty("refreshToken", refreshToken);
        expectedProperties.setProperty("coreToken", coreToken);
        expectedProperties.setProperty("clientId", clientId);
        expectedProperties.setProperty("clientSecret", clientSecret);
        expectedProperties.setProperty("internalEndpoint", internalEndpoint);
        expectedProperties.setProperty("port", port);
        expectedProperties.setProperty("tenantId", tenantId);
        expectedProperties.setProperty("dataspace", dataspace);
        expectedProperties.setProperty("coreTenantId", coreTenantId);

        val dataCloudDatasource = new DataCloudDatasource();
        dataCloudDatasource.setConnectionUrl(connectionUrl);
        dataCloudDatasource.setUserName(userName);
        dataCloudDatasource.setPassword(password);
        dataCloudDatasource.setPrivateKey(privateKey);
        dataCloudDatasource.setRefreshToken(refreshToken);
        dataCloudDatasource.setCoreToken(coreToken);
        dataCloudDatasource.setInternalEndpoint(internalEndpoint);
        dataCloudDatasource.setPort(port);
        dataCloudDatasource.setTenantId(tenantId);
        dataCloudDatasource.setDataspace(dataspace);
        dataCloudDatasource.setCoreTenantId(coreTenantId);
        dataCloudDatasource.setClientId(clientId);
        dataCloudDatasource.setClientSecret(clientSecret);
        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
            mockedDriverManager
                    .when(() -> DriverManager.getConnection(connectionUrl, expectedProperties))
                    .thenReturn(mockConnection);
            val connection = dataCloudDatasource.getConnection();
            assertThat(connection).isSameAs(mockConnection);
        }
    }

    @Test
    void testGetConnectionWithUsernameAndPasswordReturnsInstanceOfConnection() throws SQLException {
        val expectedProperties = new Properties();
        val connectionUrl = UUID.randomUUID().toString();
        val userName = UUID.randomUUID().toString();
        val password = UUID.randomUUID().toString();
        expectedProperties.setProperty("userName", userName);
        expectedProperties.setProperty("password", password);
        val dataCloudDatasource = new DataCloudDatasource();
        dataCloudDatasource.setConnectionUrl(connectionUrl);
        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
            mockedDriverManager
                    .when(() -> DriverManager.getConnection(connectionUrl, expectedProperties))
                    .thenReturn(mockConnection);
            val connection = dataCloudDatasource.getConnection(userName, password);
            assertThat(connection).isSameAs(mockConnection);
        }
    }

    private static Stream<Executable> unsupportedMethods() {
        return Stream.of(
                () -> dataCloudDatasource.setLoginTimeout(0),
                () -> dataCloudDatasource.getLoginTimeout(),
                () -> dataCloudDatasource.setLogWriter(null),
                () -> dataCloudDatasource.getLogWriter(),
                () -> dataCloudDatasource.getParentLogger());
    }

    @ParameterizedTest
    @MethodSource("unsupportedMethods")
    void throwsOnUnsupportedMethods(Executable func) {
        val ex = Assertions.assertThrows(DataCloudJDBCException.class, func);
        AssertionsForClassTypes.assertThat(ex)
                .hasMessage("Datasource method is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Test
    void unwrapMethodsActAsExpected() throws SQLException {
        val dataCloudDatasource = new DataCloudDatasource();
        Assertions.assertNull(dataCloudDatasource.unwrap(DataCloudDatasource.class));
        Assertions.assertFalse(dataCloudDatasource.isWrapperFor(DataCloudDatasource.class));
    }
}
