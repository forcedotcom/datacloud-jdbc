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
package com.salesforce.datacloud.jdbc.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.http.internal.SFDefaultSocketFactoryWrapper;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class ClientBuilderTest {

    static final Function<Properties, OkHttpClient> buildClient = ClientBuilder::buildOkHttpClient;
    Random random = new Random(10);

    @FunctionalInterface
    interface OkHttpClientTimeout {
        int getTimeout(OkHttpClient client);
    }

    static Stream<Arguments> timeoutArguments() {
        return Stream.of(
                Arguments.of("readTimeOutSeconds", 600, (OkHttpClientTimeout) OkHttpClient::readTimeoutMillis),
                Arguments.of("connectTimeOutSeconds", 600, (OkHttpClientTimeout) OkHttpClient::connectTimeoutMillis),
                Arguments.of("callTimeOutSeconds", 600, (OkHttpClientTimeout) OkHttpClient::callTimeoutMillis));
    }

    @ParameterizedTest
    @MethodSource("timeoutArguments")
    void createsClientWithAppropriateTimeouts(String key, int defaultSeconds, OkHttpClientTimeout actual) {
        val properties = new Properties();
        val none = buildClient.apply(properties);
        assertThat(actual.getTimeout(none)).isEqualTo(defaultSeconds * 1000);

        val notDefaultSeconds = defaultSeconds + random.nextInt(12345);
        properties.setProperty(key, Integer.toString(notDefaultSeconds));
        val some = buildClient.apply(properties);
        assertThat(actual.getTimeout(some)).isEqualTo(notDefaultSeconds * 1000);
    }

    @SneakyThrows
    @Test
    void createsClientWithSocketFactoryIfSocksProxyEnabled() {
        val actual = new AtomicReference<>(Optional.empty());

        try (val x = Mockito.mockConstruction(
                SFDefaultSocketFactoryWrapper.class,
                (mock, context) -> actual.set(Optional.of(context.arguments().get(0))))) {
            val client = buildClient.apply(new Properties());
            assertThat(client).isNotNull();
        }

        assertThat(actual.get()).isPresent().isEqualTo(Optional.of(false));
    }

    @SneakyThrows
    @Test
    void createsClientWithSocketFactoryIfSocksProxyDisabled() {
        val actual = new AtomicReference<>(Optional.empty());

        val properties = new Properties();
        properties.put("disableSocksProxy", "true");

        try (val x = Mockito.mockConstruction(
                SFDefaultSocketFactoryWrapper.class,
                (mock, context) -> actual.set(Optional.of(context.arguments().get(0))))) {
            val client = buildClient.apply(properties);
            assertThat(client).isNotNull();
        }

        assertThat(actual.get()).isPresent().isEqualTo(Optional.of(true));
    }

    @SneakyThrows
    @Test
    void createClientHasSomeDefaults() {
        val client = buildClient.apply(new Properties());
        assertThat(client.retryOnConnectionFailure()).isTrue();
        assertThat(client.interceptors()).hasAtLeastOneElementOfType(MetadataCacheInterceptor.class);
    }
}
