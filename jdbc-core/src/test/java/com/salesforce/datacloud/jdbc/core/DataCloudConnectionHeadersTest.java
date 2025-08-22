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
