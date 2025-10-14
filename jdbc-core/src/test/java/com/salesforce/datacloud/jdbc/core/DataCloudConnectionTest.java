/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.grpc.ClientInterceptor;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@ExtendWith(MockitoExtension.class)
class DataCloudConnectionTest extends InterceptedHyperTestBase {

    @Test
    void testCreateStatement() {
        try (val connection = getInterceptedClientConnection()) {
            val statement = connection.createStatement();
            assertThat(statement).isInstanceOf(DataCloudStatement.class);
        }
    }

    @Test
    void testClose() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.isClosed()).isFalse();
            connection.close();
            assertThat(connection.isClosed()).isTrue();
        }
    }

    @Test
    void testGetMetadata() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.getMetaData()).isInstanceOf(DataCloudDatabaseMetadata.class);
        }
    }

    @Test
    void testGetTransactionIsolation() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_NONE);
        }
    }

    @Test
    void testIsValidNegativeTimeoutThrows() {
        try (val connection = getInterceptedClientConnection()) {
            val ex = assertThrows(SQLException.class, () -> connection.isValid(-1));
            assertThat(ex).hasMessage("Invalid timeout value: -1").hasNoCause();
        }
    }

    @Test
    @SneakyThrows
    void testIsValid() {
        try (val connection = getInterceptedClientConnection()) {
            assertThat(connection.isValid(200)).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void testConnectionUnwrap() {
        val connection = getInterceptedClientConnection();
        val unwrapped = connection.unwrap(DataCloudConnection.class);
        assertThat(connection.isWrapperFor(DataCloudConnection.class)).isTrue();
        assertThat(unwrapped).isInstanceOf(DataCloudConnection.class);
        assertThrows(SQLException.class, () -> connection.unwrap(String.class));
        connection.close();
    }

    /**
     * Minimal stub provider for testing purposes.
     */
    private static class TestStubProvider implements HyperGrpcStubProvider {
        @Getter
        public final HyperServiceGrpc.HyperServiceBlockingStub stub =
                mock(HyperServiceGrpc.HyperServiceBlockingStub.class);

        @Override
        public void close() throws Exception {
            // No-op
        }
    }

    @SneakyThrows
    @Test
    void testDriverInterceptorsAreAddedWhenStubProviderIsUsed() {
        val stubProvider = new TestStubProvider();
        val connection = DataCloudConnection.of(stubProvider, ConnectionProperties.defaultProperties(), "", null);
        connection.getStub();
        // Interceptors should have been added to set the default workload header (x-hyperdb-workload)
        verify(stubProvider.stub).withInterceptors(any(ClientInterceptor[].class));
        connection.close();
    }
}
