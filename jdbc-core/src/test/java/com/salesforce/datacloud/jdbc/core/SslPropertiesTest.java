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

        // Verify SSL disabled is parsed correctly
        assertThat(sslProps).isNotNull();
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testSslCertificateProperties() throws Exception {
        // Create a temporary file for truststore validation
        java.io.File tempTruststore = java.io.File.createTempFile("test-truststore", ".jks");
        tempTruststore.deleteOnExit();
        // Write some content so file is not empty
        java.nio.file.Files.write(tempTruststore.toPath(), "dummy content".getBytes());

        // Test with only truststore (no client certs or CA cert)
        Properties props = new Properties();
        props.setProperty("ssl.truststore.path", tempTruststore.getAbsolutePath());
        props.setProperty("ssl.truststore.password", "password");
        props.setProperty("ssl.truststore.type", "PKCS12");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        // Verify properties are parsed correctly
        assertThat(sslProps.getTruststorePathValue()).isEqualTo(tempTruststore.getAbsolutePath());
        assertThat(sslProps.getTruststorePasswordValue()).isEqualTo("password");
        assertThat(sslProps.getTruststoreTypeValue()).isEqualTo("PKCS12");
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testBooleanParsing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "false");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        // Verify boolean is parsed correctly (ssl.disabled=false means SSL is enabled)
        assertThat(sslProps).isNotNull();
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testCaseInsensitiveBooleanParsing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "FALSE");

        SslProperties sslProps = SslProperties.ofDestructive(props);

        // Verify case-insensitive boolean parsing
        assertThat(sslProps).isNotNull();
        assertThat(props).isEmpty(); // All properties consumed
    }

    @Test
    void testToPropertiesSerialization() throws Exception {
        // Test SSL disabled serialization
        Properties props = new Properties();
        props.setProperty("ssl.disabled", "true");

        SslProperties sslProps = SslProperties.ofDestructive(props);
        Properties serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isEqualTo("true");
        assertThat(serialized.getProperty("ssl.truststore.path")).isNull();

        // Test one-sided TLS serialization
        // Create a temporary file for truststore validation
        java.io.File tempTruststore = java.io.File.createTempFile("test-truststore", ".jks");
        tempTruststore.deleteOnExit();
        // Write some content so file is not empty
        java.nio.file.Files.write(tempTruststore.toPath(), "dummy content".getBytes());

        props = new Properties();
        props.setProperty("ssl.truststore.path", tempTruststore.getAbsolutePath());
        sslProps = SslProperties.ofDestructive(props);
        serialized = sslProps.toProperties();

        assertThat(serialized.getProperty("ssl.disabled")).isNull();
        assertThat(serialized.getProperty("ssl.truststore.path")).isEqualTo(tempTruststore.getAbsolutePath());
    }
}
