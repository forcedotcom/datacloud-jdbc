/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

class DataCloudConnectionHeadersTest {

    private static class TestStubProvider implements HyperGrpcStubProvider {
        private final HyperServiceGrpc.HyperServiceBlockingStub stub;

        TestStubProvider() {
            this.stub = HyperServiceGrpc.newBlockingStub(InProcessChannelBuilder.forName("headers-test")
                    .usePlaintext()
                    .build());
        }

        @Override
        public HyperServiceGrpc.HyperServiceBlockingStub getStub() {
            return stub;
        }

        @Override
        public void close() {}
    }

    @Test
    void deriveHeaders_defaultsOnlyWorkload() throws DataCloudJDBCException {
        ConnectionProperties cp = ConnectionProperties.of(new Properties());
        Metadata md = DataCloudConnection.deriveHeadersFromProperties(cp);
        assertThat(md.keys()).containsExactly("x-hyperdb-workload");
    }

    @Test
    void deriveHeaders_allFieldsPresent() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("workload", "wl");
        props.setProperty("external-client-context", "{}");
        props.setProperty("dataspace", "ds");
        ConnectionProperties cp = ConnectionProperties.of(props);

        Metadata md = DataCloudConnection.deriveHeadersFromProperties(cp);
        assertThat(md.keys())
                .containsExactlyInAnyOrder("x-hyperdb-workload", "x-hyperdb-external-client-context", "dataspace");
        assertThat(md.get(Metadata.Key.of("x-hyperdb-workload", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("wl");
        assertThat(md.get(Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("{}");
        assertThat(md.get(Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER)))
                .isEqualTo("ds");
    }

    @Test
    void getStub_attachesInterceptors_andHonorsNetworkTimeout() throws DataCloudJDBCException {
        Properties props = new Properties();
        props.setProperty("workload", "wl");
        DataCloudConnection conn = DataCloudConnection.of(new TestStubProvider(), props);
        conn.setNetworkTimeout(null, 1000);
        assertThat(conn.getStub()).isNotNull();
    }

    @Test
    void oauthOverload_buildsConnection() throws DataCloudJDBCException, java.sql.SQLException {
        Properties props = new Properties();
        ClientInterceptor auth = MetadataUtils.newAttachHeadersInterceptor(new Metadata());
        DataCloudConnection conn = DataCloudConnection.of(
                InProcessChannelBuilder.forName("oauth-overload").usePlaintext(),
                props,
                auth,
                () -> "lake",
                () -> java.util.Collections.singletonList("ds"),
                DataCloudConnectionString.of("jdbc:salesforce-datacloud://login.salesforce.com"));
        assertThat(conn).isNotNull();
    }
}
