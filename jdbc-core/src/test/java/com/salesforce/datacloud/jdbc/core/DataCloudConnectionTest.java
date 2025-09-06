/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ClientInterceptor;
import java.sql.Connection;
import java.util.Properties;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@ExtendWith(MockitoExtension.class)
class DataCloudConnectionTest extends HyperGrpcTestBase {

    @Test
    void testCreateStatement() {
        try (val connection = sut()) {
            val statement = connection.createStatement();
            assertThat(statement).isInstanceOf(DataCloudStatement.class);
        }
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
        val unwrapped = connection.unwrap(DataCloudConnection.class);
        assertThat(connection.isWrapperFor(DataCloudConnection.class)).isTrue();
        assertThat(unwrapped).isInstanceOf(DataCloudConnection.class);
        assertThrows(DataCloudJDBCException.class, () -> connection.unwrap(String.class));
        connection.close();
    }

    @SneakyThrows
    @Test
    void testChannelClosesWhenShouldCloseChannelWithConnectionIsTrue() {
        val mockChannel = mock(DataCloudJdbcManagedChannel.class);
        val connection = DataCloudConnection.of(new JdbcDriverStubProvider(mockChannel, true), new Properties());

        connection.close();

        verify(mockChannel).close();
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
        val properties = new Properties();
        val stubProvider = new TestStubProvider();
        val connection = DataCloudConnection.of(stubProvider, properties);
        connection.getStub();
        // Interceptors should have been added to set the default workload header (x-hyperdb-workload)
        verify(stubProvider.stub).withInterceptors(any(ClientInterceptor[].class));
        connection.close();
    }

    @SneakyThrows
    @Test
    void testChannelNotClosedWhenShouldCloseChannelWithConnectionIsFalse() {
        val mockChannel = mock(DataCloudJdbcManagedChannel.class);
        val connection = DataCloudConnection.of(new JdbcDriverStubProvider(mockChannel, false), new Properties());

        connection.close();

        verify(mockChannel, never()).close();
    }

    @SneakyThrows
    private DataCloudConnection sut() {
        return DataCloudConnection.of(stubProvider, new Properties());
    }
}
