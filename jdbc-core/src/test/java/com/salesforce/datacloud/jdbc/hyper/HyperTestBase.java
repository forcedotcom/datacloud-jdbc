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
package com.salesforce.datacloud.jdbc.hyper;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.interceptor.AuthorizationHeaderInterceptor;
import io.grpc.ManagedChannelBuilder;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HyperTestBase implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    private static final HyperServerProcess instance = new HyperServerProcess();

    private static boolean isRegistered = false;


    @SneakyThrows
    public static void assertEachRowIsTheSame(ResultSet rs, AtomicInteger prev) {
        val expected = prev.incrementAndGet();
        val a = rs.getBigDecimal(1).intValue();
        assertThat(expected).isEqualTo(a);
    }

    @SafeVarargs
    @SneakyThrows
    public static void assertWithConnection(
            ThrowingConsumer<DataCloudConnection> assertion, Map.Entry<String, String>... settings) {
        try (val connection =
                getHyperQueryConnection(settings == null ? ImmutableMap.of() : ImmutableMap.ofEntries(settings))) {
            assertion.accept(connection);
        }
    }

    @SafeVarargs
    @SneakyThrows
    public static void assertWithStatement(
            ThrowingConsumer<DataCloudStatement> assertion, Map.Entry<String, String>... settings) {
        try (val connection = getHyperQueryConnection(
                        settings == null ? ImmutableMap.of() : ImmutableMap.ofEntries(settings));
                val result = connection.createStatement().unwrap(DataCloudStatement.class)) {
            assertion.accept(result);
        }
    }

    public static DataCloudConnection getHyperQueryConnection() {
        return getHyperQueryConnection(ImmutableMap.of());
    }

    @SneakyThrows
    public static DataCloudConnection getHyperQueryConnection(Map<String, String> connectionSettings) {
        val properties = new Properties();
        properties.putAll(connectionSettings);
        val port = getInstancePort();
        log.info("Creating connection to port {}", port);
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext();
        return DataCloudConnection.fromChannel(channel, properties);
    }

    public static int getInstancePort() {
        val hyper = getInstance();
        Assertions.assertTrue((hyper != null) && hyper.isHealthy(), "Hyper wasn't started, failing test");
        return hyper.getPort();
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (HyperTestBase.class) {
            if (!isRegistered) {
                context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL)
                        .put(HyperTestBase.class.getName(), this);
                isRegistered = true;
                System.out.println("Registered database shutdown hook");
            }
        }
    }

    @Override
    @Timeout(5_000)
    public void close() throws Throwable {
        val instance = getInstance();
        if (instance != null) {
            instance.close();
        }
    }

    public static class NoopTokenSupplier implements AuthorizationHeaderInterceptor.TokenSupplier {
        @Override
        public String getToken() {
            return "";
        }
    }
}
