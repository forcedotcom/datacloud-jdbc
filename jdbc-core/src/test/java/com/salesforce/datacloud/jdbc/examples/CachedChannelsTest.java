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
package com.salesforce.datacloud.jdbc.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudJdbcManagedChannel;
import com.salesforce.datacloud.jdbc.core.HyperGrpcStubProvider;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc.HyperServiceBlockingStub;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class CachedChannelsTest {
    /**
     * This example shows how you can use the stub provider to reuse a channel across multiple JDBC Connections.
     */
    @Test
    public void reuseChannelAcrossConnections() throws SQLException {
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels that are set up in the way you like (mTLS / Plaintext / ...) and your
        // own interceptors as well as executors.
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();
        try (DataCloudJdbcManagedChannel jdbcManagedChannel =
                DataCloudJdbcManagedChannel.of(channelBuilder, properties)) {
            // This is the first connection that uses this channel
            try (DataCloudConnection conn =
                    DataCloudConnection.of(new JdbcDriverStubProvider(jdbcManagedChannel, false), properties)) {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(1,10) s");
                    while (rs.next()) {
                        System.out.println("Retrieved value for first query:" + rs.getLong(1));
                    }
                }
            }

            // This is the second connection that uses the same channel
            try (DataCloudConnection conn =
                    DataCloudConnection.of(new JdbcDriverStubProvider(jdbcManagedChannel, false), properties)) {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(20,30) s");
                    while (rs.next()) {
                        System.out.println("Retrieved value for second query:" + rs.getLong(1));
                    }
                }
            }
        }
    }

    /**
     * This example shows how you can use the stub provider to reuse a channel while having different interceptors per JDBC Connection.
     */
    @Test
    public void reuseChannelWithCustomStubInterceptors() throws SQLException {
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels that are set up in the way you like (mTLS / Plaintext / ...) and your
        // own channel-level interceptors as well as executors.
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();
        ManagedChannel managedChannel = channelBuilder.build();
        try {
            // This is the first connection that uses this channel and it has a custom interceptor that sets the
            // external-client-context to "123"
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER), "123");
            ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(metadata);
            try (DataCloudConnection conn =
                    DataCloudConnection.of(new InterceptorStubProvider(managedChannel, interceptor), properties)) {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SHOW external_client_context");
                    rs.next();
                    System.out.println("Retrieved value for first query:" + rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("123");
                }
            }

            // This is the second connection that uses this channel and it has a custom interceptor that sets the
            // external-client-context to "456"
            Metadata metadata2 = new Metadata();
            metadata2.put(
                    Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER), "456");
            ClientInterceptor interceptor2 = MetadataUtils.newAttachHeadersInterceptor(metadata2);
            try (DataCloudConnection conn =
                    DataCloudConnection.of(new InterceptorStubProvider(managedChannel, interceptor2), properties)) {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SHOW external_client_context");
                    rs.next();
                    System.out.println("Retrieved value for first query:" + rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("456");
                }
            }
        } finally {
            managedChannel.shutdown();
        }
    }

    /**
     * This class provides a stub for the Hyper gRPC client used by the JDBC Connection.
     * It creates the stub with the provided interceptors applied.
     */
    private static class InterceptorStubProvider implements HyperGrpcStubProvider {
        private final ManagedChannel channel;
        private final ClientInterceptor[] interceptors;

        /**
         * Initializes the stub provider with the provided channel and interceptors that should be applied to all stubs.
         * @param channel The channel to use for the stub
         * @param interceptors The interceptors to apply to the stub
         */
        public InterceptorStubProvider(ManagedChannel channel, ClientInterceptor... interceptors) {
            this.channel = channel;
            this.interceptors = interceptors;
        }

        /** Returns a stub with the configured interceptors applied. */
        @Override
        public HyperServiceBlockingStub getStub() {
            return HyperServiceGrpc.newBlockingStub(channel).withInterceptors(interceptors);
        }

        @Override
        public void close() throws Exception {
            // No-op
        }
    }
}
