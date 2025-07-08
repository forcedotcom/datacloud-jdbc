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
package com.salesforce.datacloud.jdbc.interceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudJdbcManagedChannel;
import com.salesforce.datacloud.jdbc.core.HyperGrpcStubProvider;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@Slf4j
@ExtendWith(HyperTestBase.class)
class DuplicateKeyDetectionInterceptorTest {
    HeaderMutatingClientInterceptor a = metadata -> {
        log.warn("test interceptor attaching 'dataspace' with 'bar'");
        metadata.put(Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER), "bar");
    };

    HeaderMutatingClientInterceptor b = metadata -> {
        log.warn("test interceptor attaching 'dataspace' with 'baz'");
        metadata.put(Metadata.Key.of("dataspace", Metadata.ASCII_STRING_MARSHALLER), "baz");
    };

    @Test
    void shouldThrowWhenDuplicatesFromStubProvider() throws Exception {
        val properties = new Properties();
        properties.setProperty("dataspace", "foo");

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        val stubProvider = new JdbcDriverStubProvider(DataCloudJdbcManagedChannel.of(builder, properties), true);

        HyperGrpcStubProvider provider = new HyperGrpcStubProvider() {
            @Override
            public HyperServiceGrpc.HyperServiceBlockingStub getStub() {
                return stubProvider.getStub().withInterceptors(a, b);
            }

            @Override
            public void close() throws Exception {
                stubProvider.close();
            }
        };

        try (val conn = DataCloudConnection.of(provider, properties)) {
            makeAssertions(conn);
        }

    }


    @Test
    void shouldThrowWhenDuplicatesFromChannelBuilder() throws SQLException {
        val properties = new Properties();
        properties.setProperty("dataspace", "foo");

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .intercept(a, b)
                .usePlaintext();

        try (val conn = DataCloudConnection.of(channelBuilder, properties)) {
            makeAssertions(conn);
        }
    }

    @Test
    void shouldThrowWhenUserAppliesDuplicateDetector() throws SQLException {
        val properties = new Properties();
        properties.setProperty("dataspace", "foo");

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .intercept(new DuplicateKeyDetectionInterceptor("from test"), a, b)
                .usePlaintext();

        try (val conn = DataCloudConnection.of(channelBuilder, properties)) {
            makeAssertions(conn);
        }
    }

    private void makeAssertions(DataCloudConnection connection) {
        assertThatThrownBy(() -> {
            try (val stmt = connection.createStatement()) {
                val rs = stmt.executeQuery("SELECT 1");
                assertSelectOne(rs);
            }
        })                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Duplicate metadata keys detected");
    }

    private void assertSelectOne(final ResultSet rs) throws SQLException {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
    }
}
