/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.http.internal.SocketFactoryWrapper;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.val;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HttpClientPropertiesTest {
    @Test
    void testParseFromPropertiesWithAllSettings() throws SQLException {
        Properties props = new Properties();
        props.setProperty(HttpClientProperties.HTTP_LOG_LEVEL, "BODY");
        props.setProperty(HttpClientProperties.HTTP_READ_TIMEOUT_SECONDS, "300");
        props.setProperty(HttpClientProperties.HTTP_CONNECT_TIMEOUT_SECONDS, "30");
        props.setProperty(HttpClientProperties.HTTP_CALL_TIMEOUT_SECONDS, "120");
        props.setProperty(HttpClientProperties.HTTP_DISABLE_SOCKS_PROXY, "true");
        props.setProperty(HttpClientProperties.HTTP_CACHE_TTL_MS, "5000");
        props.setProperty(HttpClientProperties.HTTP_MAX_RETRIES, "5");
        props.setProperty("otherSetting", "foobar");

        HttpClientProperties httpProps = HttpClientProperties.ofDestructive(props);

        assertThat(httpProps.getLogLevel()).isEqualTo(HttpLoggingInterceptor.Level.BODY);
        assertThat(httpProps.getReadTimeoutSeconds()).isEqualTo(300);
        assertThat(httpProps.getConnectTimeoutSeconds()).isEqualTo(30);
        assertThat(httpProps.getCallTimeoutSeconds()).isEqualTo(120);
        assertThat(httpProps.isDisableSocksProxy()).isTrue();
        assertThat(httpProps.getMetadataCacheTtlMs()).isEqualTo(5000);
        assertThat(httpProps.getMaxRetries()).isEqualTo(5);

        // Properties should be removed after parsing
        assertThat(props).size().isEqualTo(1);
        assertThat(props).containsEntry("otherSetting", "foobar");
    }

    @Test
    void testParseFromPropertiesWithPartialSettings() throws SQLException {
        Properties props = new Properties();
        props.setProperty(HttpClientProperties.HTTP_LOG_LEVEL, "HEADERS");
        props.setProperty(HttpClientProperties.HTTP_READ_TIMEOUT_SECONDS, "900");
        props.setProperty(HttpClientProperties.HTTP_DISABLE_SOCKS_PROXY, "false");

        HttpClientProperties httpProps = HttpClientProperties.ofDestructive(props);

        assertThat(httpProps.getLogLevel()).isEqualTo(HttpLoggingInterceptor.Level.HEADERS);
        assertThat(httpProps.getReadTimeoutSeconds()).isEqualTo(900);
        assertThat(httpProps.isDisableSocksProxy()).isFalse();

        // Should use defaults for unspecified properties
        assertThat(httpProps.getConnectTimeoutSeconds()).isEqualTo(600);
        assertThat(httpProps.getCallTimeoutSeconds()).isEqualTo(600);
        assertThat(httpProps.getMetadataCacheTtlMs()).isEqualTo(10000);
        assertThat(httpProps.getMaxRetries()).isEqualTo(3);

        // Properties should be removed after parsing
        assertThat(props).isEmpty();
    }

    @Test
    void testParseFromEmptyProperties() throws SQLException {
        Properties props = new Properties();

        HttpClientProperties httpProps = HttpClientProperties.ofDestructive(props);

        // Should use all defaults
        assertThat(httpProps.getLogLevel()).isEqualTo(HttpLoggingInterceptor.Level.BASIC);
        assertThat(httpProps.getReadTimeoutSeconds()).isEqualTo(600);
        assertThat(httpProps.getConnectTimeoutSeconds()).isEqualTo(600);
        assertThat(httpProps.getCallTimeoutSeconds()).isEqualTo(600);
        assertThat(httpProps.isDisableSocksProxy()).isFalse();
        assertThat(httpProps.getMetadataCacheTtlMs()).isEqualTo(10000);
        assertThat(httpProps.getMaxRetries()).isEqualTo(3);

        // Properties should remain empty
        assertThat(props).isEmpty();
    }

    @Test
    void testRoundtripConversion() throws SQLException {
        Properties originalProps = new Properties();
        originalProps.setProperty(HttpClientProperties.HTTP_LOG_LEVEL, "BODY");
        originalProps.setProperty(HttpClientProperties.HTTP_READ_TIMEOUT_SECONDS, "300");
        originalProps.setProperty(HttpClientProperties.HTTP_CONNECT_TIMEOUT_SECONDS, "30");
        originalProps.setProperty(HttpClientProperties.HTTP_CALL_TIMEOUT_SECONDS, "120");
        originalProps.setProperty(HttpClientProperties.HTTP_DISABLE_SOCKS_PROXY, "true");
        originalProps.setProperty(HttpClientProperties.HTTP_CACHE_TTL_MS, "5000");
        originalProps.setProperty(HttpClientProperties.HTTP_MAX_RETRIES, "5");

        // Parse to HttpClientProperties
        HttpClientProperties httpProps = HttpClientProperties.ofDestructive(originalProps);

        // Convert back to Properties
        Properties roundtripProps = httpProps.toProperties();

        // Verify round-trip preserves non-default values
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_LOG_LEVEL))
                .isEqualTo("BODY");
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_READ_TIMEOUT_SECONDS))
                .isEqualTo("300");
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_CONNECT_TIMEOUT_SECONDS))
                .isEqualTo("30");
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_CALL_TIMEOUT_SECONDS))
                .isEqualTo("120");
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_DISABLE_SOCKS_PROXY))
                .isEqualTo("true");
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_CACHE_TTL_MS))
                .isEqualTo("5000");
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_MAX_RETRIES))
                .isEqualTo("5");
    }

    @Test
    void testRoundtripConversionWithDefaults() throws SQLException {
        Properties originalProps = new Properties();
        // Only set one non-default value
        originalProps.setProperty(HttpClientProperties.HTTP_LOG_LEVEL, "NONE");

        // Parse to HttpClientProperties
        HttpClientProperties httpProps = HttpClientProperties.ofDestructive(originalProps);

        // Convert back to Properties
        Properties roundtripProps = httpProps.toProperties();

        // Should only contain the non-default value
        assertThat(roundtripProps.size()).isEqualTo(1);
        assertThat(roundtripProps.getProperty(HttpClientProperties.HTTP_LOG_LEVEL))
                .isEqualTo("NONE");
    }

    @Test
    void testBuildOkHttpClient() throws SQLException {
        Properties props = new Properties();
        props.setProperty(HttpClientProperties.HTTP_LOG_LEVEL, "BODY");
        props.setProperty(HttpClientProperties.HTTP_READ_TIMEOUT_SECONDS, "300");
        props.setProperty(HttpClientProperties.HTTP_CONNECT_TIMEOUT_SECONDS, "30");
        props.setProperty(HttpClientProperties.HTTP_CALL_TIMEOUT_SECONDS, "120");
        props.setProperty(HttpClientProperties.HTTP_DISABLE_SOCKS_PROXY, "true");
        props.setProperty(HttpClientProperties.HTTP_CACHE_TTL_MS, "5000");

        HttpClientProperties httpProps = HttpClientProperties.ofDestructive(props);
        OkHttpClient client = httpProps.buildOkHttpClient();

        assertThat(client).isNotNull();
        assertThat(client.readTimeoutMillis()).isEqualTo(300000); // 300 seconds
        assertThat(client.connectTimeoutMillis()).isEqualTo(30000); // 30 seconds
        assertThat(client.callTimeoutMillis()).isEqualTo(120000); // 120 seconds
    }

    @SneakyThrows
    @Test
    void createsClientWithSocketFactoryIfSocksProxyEnabled() {
        val properties = new Properties();

        val actual = new AtomicReference<>(Optional.empty());
        try (val x = Mockito.mockConstruction(
                SocketFactoryWrapper.class,
                (mock, context) -> actual.set(Optional.of(context.arguments().get(0))))) {
            val httpProps = HttpClientProperties.ofDestructive(properties);
            val client = httpProps.buildOkHttpClient();
            assertThat(client).isNotNull();
        }

        assertThat(actual.get()).isPresent().isEqualTo(Optional.of(false));
    }

    @SneakyThrows
    @Test
    void createsClientWithSocketFactoryIfSocksProxyDisabled() {

        val properties = new Properties();
        properties.put(HttpClientProperties.HTTP_DISABLE_SOCKS_PROXY, "true");

        val actual = new AtomicReference<>(Optional.empty());
        try (val x = Mockito.mockConstruction(
                SocketFactoryWrapper.class,
                (mock, context) -> actual.set(Optional.of(context.arguments().get(0))))) {
            val httpProps = HttpClientProperties.ofDestructive(properties);
            val client = httpProps.buildOkHttpClient();
            assertThat(client).isNotNull();
        }

        assertThat(actual.get()).isPresent().isEqualTo(Optional.of(true));
    }
}
