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

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class PerQueryInterceptorTest {
    /**
     * This example shows how to provide per query header overrides through an interceptor
     */
    @Test
    public void testUpdateableInterceptor() throws SQLException {
        // The interceptor
        HeaderClientInterceptor interceptor = new HeaderClientInterceptor();
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels, setup in the way you like (mTLS / Plaintext / ...) and your own
        // interceptors as well as executors.
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .intercept(interceptor)
                .usePlaintext();

        // Use the JDBC Driver interface
        try (DataCloudConnection conn = DataCloudConnection.fromChannel(channel, properties)) {
            for (int i = 0; i < 3; i++) {
                try (Statement stmt = conn.createStatement()) {
                    interceptor.setValue(String.valueOf(i));
                    ResultSet rs = stmt.executeQuery("SHOW external_client_context");
                    Assertions.assertTrue(rs.next());
                }
            }
        }
    }

    /**
     * This is a sample interceptor that updates some headers. The value can be updated through the set method.
     * It uses synchronized as a simple way to ensure correctness under potential multi threading
     */
    private static class HeaderClientInterceptor implements ClientInterceptor {
        static final Metadata.Key<String> HYPER_HEADER =
                Metadata.Key.of("x-hyperdb-external-client-context", Metadata.ASCII_STRING_MARSHALLER);

        private String value;

        public synchronized void setValue(String value_) {
            value = value_;
        }

        @Override
        public synchronized <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    if (value != null) {
                        headers.removeAll(HYPER_HEADER);
                        headers.put(HYPER_HEADER, value);
                    }
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
