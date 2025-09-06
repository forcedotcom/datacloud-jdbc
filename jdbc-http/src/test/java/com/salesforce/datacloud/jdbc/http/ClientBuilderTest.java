/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
