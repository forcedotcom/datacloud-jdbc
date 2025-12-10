/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

class DataCloudConnectionHeadersTest {

    private static class TestStubProvider implements HyperGrpcStubProvider {
        private final HyperServiceGrpc.HyperServiceStub stub;

        TestStubProvider() {
            this.stub = HyperServiceGrpc.newStub(InProcessChannelBuilder.forName("headers-test")
                    .usePlaintext()
                    .build());
        }

        @Override
        public HyperServiceGrpc.HyperServiceStub getStub() {
            return stub;
        }

        @Override
        public void close() {
            ((ManagedChannel) stub.getChannel()).shutdownNow();
        }
    }

    @Test
    void deriveHeaders_defaultsOnlyWorkload() throws SQLException {
        ConnectionProperties cp = ConnectionProperties.ofDestructive(new Properties());
        Metadata md = DataCloudConnection.deriveHeadersFromProperties(cp);
        assertThat(md.keys()).containsExactly("user-agent", "x-hyperdb-workload");
    }

    @Test
    void deriveHeaders_allFieldsPresent() throws SQLException {
        Properties props = new Properties();
        props.setProperty("workload", "wl");
        props.setProperty("externalClientContext", "{}");
        ConnectionProperties cp = ConnectionProperties.ofDestructive(props);

        Metadata md = DataCloudConnection.deriveHeadersFromProperties(cp);
        assertThat(md.keys())
                .containsExactlyInAnyOrder("user-agent", "x-hyperdb-workload", "x-hyperdb-external-client-context");
        assertThat(md.get(Metadata.Key.of("x-hyperdb-workload", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("wl");
        assertThat(md.get(Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("{}");
    }

    @Test
    void getStub_attachesInterceptors_andHonorsNetworkTimeout() throws SQLException {
        Properties props = new Properties();
        props.setProperty("workload", "wl");
        try (DataCloudConnection conn =
                DataCloudConnection.of(new TestStubProvider(), ConnectionProperties.ofDestructive(props), null)) {
            conn.setNetworkTimeout(null, 1000);
            assertThat(conn.getStub()).isNotNull();
        }
    }
}
