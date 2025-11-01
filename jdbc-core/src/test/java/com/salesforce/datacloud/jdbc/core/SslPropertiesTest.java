/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests for SslProperties class.
 */
class SslPropertiesTest {

    @Test
    void testDefaultValues() throws SQLException {
        SslProperties props = SslProperties.defaultProperties();

        assertThat(props.getSslMode()).isEqualTo(SslProperties.SslMode.DEFAULT_TLS);
        assertThat(props.getTruststorePathValue()).isNull();
        assertThat(props.getTruststorePasswordValue()).isNull();
        assertThat(props.getTruststoreTypeValue()).isEqualTo("JKS");
        assertThat(props.getClientCertPathValue()).isNull();
        assertThat(props.getClientKeyPathValue()).isNull();
        assertThat(props.getCaCertPathValue()).isNull();
    }

    @Test
    void testSslDisabledProperty() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.DISABLED);
    }

    @Test
    void testSslCertificateProperties() throws SQLException {
        // Test with only truststore (no client certs or CA cert) to avoid file validation
        Properties props = new Properties();
        props.setProperty("ssl.truststore.path", "/path/to/truststore.jks");
        props.setProperty("ssl.truststore.password", "password");
        props.setProperty("ssl.truststore.type", "PKCS12");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.ONE_SIDED_TLS);
        assertThat(sslProps.getTruststorePathValue()).isEqualTo("/path/to/truststore.jks");
        assertThat(sslProps.getTruststorePasswordValue()).isEqualTo("password");
        assertThat(sslProps.getTruststoreTypeValue()).isEqualTo("PKCS12");
    }

    @Test
    void testBooleanParsing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "false");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.DEFAULT_TLS);
    }

    @Test
    void testCaseInsensitiveBooleanParsing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "FALSE");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.DEFAULT_TLS);
    }

    @Test
    void testToPropertiesSerialization() throws SQLException {
        // Test SSL disabled serialization
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        Properties serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isEqualTo("true");
        assertThat(serialized.getProperty("ssl.truststore.path")).isNull();

        // Test one-sided TLS serialization
        props.remove("ssl.disabled");
        props.setProperty("ssl.truststore.path", "/path/to/truststore.jks");
        sslProps = SslProperties.ofDestructive(props);
        serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isNull();
        assertThat(serialized.getProperty("ssl.truststore.path")).isEqualTo("/path/to/truststore.jks");
    }

    @Test
    void testDetermineSslMode() throws SQLException {
        // Test SSL disabled
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");
        SslProperties sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.DISABLED);

        // Test default TLS (system truststore)
        props.remove("ssl.disabled");
        sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.DEFAULT_TLS);

        // Test one-sided TLS with custom truststore
        props.setProperty("ssl.truststore.path", "/path/to/truststore.jks");
        sslProps = SslProperties.ofDestructive(props);
        assertThat(sslProps.getSslMode()).isEqualTo(SslProperties.SslMode.ONE_SIDED_TLS);

        // Note: We can't test mutual TLS without real certificate files due to validation
        // This is expected behavior - validation happens during parsing
    }
}
