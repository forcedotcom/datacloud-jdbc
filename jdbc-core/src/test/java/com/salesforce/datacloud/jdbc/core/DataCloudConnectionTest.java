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
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.auth.PropertiesUtils.propertiesForPassword;
import static com.salesforce.datacloud.jdbc.util.Messages.ILLEGAL_CONNECTION_PROTOCOL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.Connection;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataCloudConnectionTest extends HyperGrpcTestBase {

    static final Properties properties = propertiesForPassword("un", "pw");

    @Test
    void testCreateStatement() {
        try (val connection = sut()) {
            val statement = connection.createStatement();
            assertThat(statement).isInstanceOf(DataCloudStatement.class);
        }
    }

    @Test
    void testNullUrlThrows() {
        Assertions.assertThrows(DataCloudJDBCException.class, () -> DataCloudConnection.of(null, new Properties()));
    }

    @Test
    void testUnsupportedPrefixUrlNotAllowed() {
        val ex = assertThrows(DataCloudJDBCException.class, () -> DataCloudConnection.of("fake-url", new Properties()));
        assertThat(ex).hasMessage(ILLEGAL_CONNECTION_PROTOCOL);
    }

    @Test
    void testClose() {
        try (val connection = sut()) {
            assertThat(connection.isClosed()).isFalse();
            connection.close();
            assertThat(connection.isClosed()).isTrue();
        }
    }

    @Test
    void testGetMetadata() {
        try (val connection = sut()) {
            assertThat(connection.getMetaData()).isInstanceOf(DataCloudDatabaseMetadata.class);
        }
    }

    @Test
    void testGetTransactionIsolation() {
        try (val connection = sut()) {
            assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_NONE);
        }
    }

    @Test
    void testIsValidNegativeTimeoutThrows() {
        try (val connection = sut()) {
            val ex = assertThrows(DataCloudJDBCException.class, () -> connection.isValid(-1));
            assertThat(ex).hasMessage("Invalid timeout value: -1").hasNoCause();
        }
    }

    @Test
    @SneakyThrows
    void testIsValid() {
        try (val connection = sut()) {
            assertThat(connection.isValid(200)).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void testConnectionUnwrap() {
        val connection = sut();
        DataCloudConnection query_conn = connection.unwrap(DataCloudConnection.class);
        assertThat(connection.isWrapperFor(DataCloudConnection.class)).isTrue();
        assertThrows(DataCloudJDBCException.class, () -> connection.unwrap(String.class));
        connection.close();
    }

    private DataCloudConnection sut() {
        return DataCloudConnection.builder()
                .executor(hyperGrpcClient)
                .tokenProcessor(mockSession)
                .properties(properties)
                .build();
    }
}
