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
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TestDirectConnectionProperties {

    @Test
    void testParsingAllProperties() throws DataCloudJDBCException {
        // Test parsing all supported direct connection properties
        Properties properties = new Properties();
        properties.setProperty("direct", "true");
        properties.setProperty("ssl_disabled", "true");
        properties.setProperty("truststore_path", "/path/to/truststore.jks");
        properties.setProperty("truststore_password", "password123");
        properties.setProperty("truststore_type", "PKCS12");
        properties.setProperty("client_cert_path", "/path/to/client.pem");
        properties.setProperty("client_key_path", "/path/to/client.key");
        properties.setProperty("ca_cert_path", "/path/to/ca.pem");

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(properties);

        // Verify all properties are parsed correctly
        assertThat(directProps.isDirectConnection()).isTrue();
        assertThat(directProps.isSslDisabledFlag()).isTrue();
        assertThat(directProps.getTruststorePathValue()).isEqualTo("/path/to/truststore.jks");
        assertThat(directProps.getTruststorePasswordValue()).isEqualTo("password123");
        assertThat(directProps.getTruststoreTypeValue()).isEqualTo("PKCS12");
        assertThat(directProps.getClientCertPathValue()).isEqualTo("/path/to/client.pem");
        assertThat(directProps.getClientKeyPathValue()).isEqualTo("/path/to/client.key");
        assertThat(directProps.getCaCertPathValue()).isEqualTo("/path/to/ca.pem");
    }

    @Test
    void testParsingWithDefaults() throws DataCloudJDBCException {
        // Test parsing with empty properties to verify defaults
        Properties properties = new Properties();

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(properties);

        // Verify default values
        assertThat(directProps.isDirectConnection()).isFalse();
        assertThat(directProps.isSslDisabledFlag()).isFalse();
        assertThat(directProps.getTruststorePathValue()).isEmpty();
        assertThat(directProps.getTruststorePasswordValue()).isEmpty();
        assertThat(directProps.getTruststoreTypeValue()).isEqualTo("JKS"); // Default truststore type
        assertThat(directProps.getClientCertPathValue()).isEmpty();
        assertThat(directProps.getClientKeyPathValue()).isEmpty();
        assertThat(directProps.getCaCertPathValue()).isEmpty();
    }

    @Test
    void testParsingPartialProperties() throws DataCloudJDBCException {
        // Test parsing with only some properties set
        Properties properties = new Properties();
        properties.setProperty("direct", "true");
        properties.setProperty("truststore_path", "/path/to/truststore.jks");
        properties.setProperty("ca_cert_path", "/path/to/ca.pem");

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(properties);

        // Verify set properties
        assertThat(directProps.isDirectConnection()).isTrue();
        assertThat(directProps.getTruststorePathValue()).isEqualTo("/path/to/truststore.jks");
        assertThat(directProps.getCaCertPathValue()).isEqualTo("/path/to/ca.pem");

        // Verify unset properties have defaults
        assertThat(directProps.isSslDisabledFlag()).isFalse();
        assertThat(directProps.getTruststorePasswordValue()).isEmpty();
        assertThat(directProps.getTruststoreTypeValue()).isEqualTo("JKS");
        assertThat(directProps.getClientCertPathValue()).isEmpty();
        assertThat(directProps.getClientKeyPathValue()).isEmpty();
    }

    @Test
    void testRoundtripSerialization() throws DataCloudJDBCException {
        // Test that parsing and serializing properties maintains data integrity
        // Note: Only non-default values are serialized, similar to ConnectionProperties behavior
        Properties originalProperties = new Properties();
        originalProperties.setProperty("direct", "true");
        originalProperties.setProperty("ssl_disabled", "true"); // Use true so it gets serialized
        originalProperties.setProperty("truststore_path", "/path/to/truststore.jks");
        originalProperties.setProperty("truststore_password", "mypassword");
        originalProperties.setProperty("truststore_type", "PKCS12");
        originalProperties.setProperty("client_cert_path", "/path/to/client.pem");
        originalProperties.setProperty("client_key_path", "/path/to/client.key");
        originalProperties.setProperty("ca_cert_path", "/path/to/ca.pem");

        // Parse into structured object
        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(originalProperties);

        // Serialize back to Properties
        Properties roundtripProperties = directProps.toProperties();

        // Verify all original properties are preserved
        assertThat(roundtripProperties.entrySet()).containsExactlyInAnyOrderElementsOf(originalProperties.entrySet());
    }

    @Test
    void testRoundtripWithDefaults() throws DataCloudJDBCException {
        // Test roundtrip with minimal properties (most using defaults)
        Properties originalProperties = new Properties();
        originalProperties.setProperty("direct", "true");
        originalProperties.setProperty("truststore_path", "/path/to/truststore.jks");

        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(originalProperties);
        Properties roundtripProperties = directProps.toProperties();

        // Should only contain non-default values
        assertThat(roundtripProperties).hasSize(2);
        assertThat(roundtripProperties.getProperty("direct")).isEqualTo("true");
        assertThat(roundtripProperties.getProperty("truststore_path")).isEqualTo("/path/to/truststore.jks");

        // Default values should not be serialized
        assertThat(roundtripProperties.getProperty("ssl_disabled")).isNull(); // false is default
        assertThat(roundtripProperties.getProperty("truststore_type")).isNull(); // JKS is default
    }

    @Test
    void testSerializationSkipsDefaults() throws DataCloudJDBCException {
        // Test that serialization only includes non-default values
        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.builder()
                .directConnection(true)
                .sslDisabledFlag(false) // default
                .truststorePathValue("/path/to/truststore.jks")
                .truststorePasswordValue("") // default (empty)
                .truststoreTypeValue("JKS") // default
                .clientCertPathValue("") // default (empty)
                .clientKeyPathValue("") // default (empty)
                .caCertPathValue("") // default (empty)
                .build();

        Properties serialized = directProps.toProperties();

        // Should only contain non-default values
        assertThat(serialized).hasSize(2);
        assertThat(serialized.getProperty("direct")).isEqualTo("true");
        assertThat(serialized.getProperty("truststore_path")).isEqualTo("/path/to/truststore.jks");
    }

    @Test
    void testParsingNullProperties() throws DataCloudJDBCException {
        // Test that null properties returns default instance
        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.of(null);

        // Verify all defaults
        assertThat(directProps.isDirectConnection()).isFalse();
        assertThat(directProps.isSslDisabledFlag()).isFalse();
        assertThat(directProps.getTruststorePathValue()).isEmpty();
        assertThat(directProps.getTruststorePasswordValue()).isEmpty();
        assertThat(directProps.getTruststoreTypeValue()).isEqualTo("JKS");
        assertThat(directProps.getClientCertPathValue()).isEmpty();
        assertThat(directProps.getClientKeyPathValue()).isEmpty();
        assertThat(directProps.getCaCertPathValue()).isEmpty();
    }

    @Test
    void testBooleanParsing() throws DataCloudJDBCException {
        // Test various boolean values
        Properties properties1 = new Properties();
        properties1.setProperty("direct", "TRUE");
        properties1.setProperty("ssl_disabled", "False");

        DirectDataCloudConnectionProperties directProps1 = DirectDataCloudConnectionProperties.of(properties1);
        assertThat(directProps1.isDirectConnection()).isTrue();
        assertThat(directProps1.isSslDisabledFlag()).isFalse();

        Properties properties2 = new Properties();
        properties2.setProperty("direct", "1"); // Should parse as false (not "true")
        properties2.setProperty("ssl_disabled", "yes"); // Should parse as false (not "true")

        DirectDataCloudConnectionProperties directProps2 = DirectDataCloudConnectionProperties.of(properties2);
        assertThat(directProps2.isDirectConnection()).isFalse();
        assertThat(directProps2.isSslDisabledFlag()).isFalse();
    }

    @Test
    void testPropertyConstants() {
        // Test that property constants are correctly defined
        assertThat(DirectDataCloudConnectionProperties.direct).isEqualTo("direct");
        assertThat(DirectDataCloudConnectionProperties.sslDisabled).isEqualTo("ssl_disabled");
        assertThat(DirectDataCloudConnectionProperties.truststorePath).isEqualTo("truststore_path");
        assertThat(DirectDataCloudConnectionProperties.truststorePassword).isEqualTo("truststore_password");
        assertThat(DirectDataCloudConnectionProperties.truststoreType).isEqualTo("truststore_type");
        assertThat(DirectDataCloudConnectionProperties.clientCertPath).isEqualTo("client_cert_path");
        assertThat(DirectDataCloudConnectionProperties.clientKeyPath).isEqualTo("client_key_path");
        assertThat(DirectDataCloudConnectionProperties.caCertPath).isEqualTo("ca_cert_path");
    }

    @Test
    void testBuilderPattern() {
        // Test that Lombok builder works correctly
        DirectDataCloudConnectionProperties directProps = DirectDataCloudConnectionProperties.builder()
                .directConnection(true)
                .sslDisabledFlag(true)
                .truststorePathValue("/custom/path")
                .truststorePasswordValue("secret")
                .truststoreTypeValue("PKCS12")
                .clientCertPathValue("/cert/path")
                .clientKeyPathValue("/key/path")
                .caCertPathValue("/ca/path")
                .build();

        assertThat(directProps.isDirectConnection()).isTrue();
        assertThat(directProps.isSslDisabledFlag()).isTrue();
        assertThat(directProps.getTruststorePathValue()).isEqualTo("/custom/path");
        assertThat(directProps.getTruststorePasswordValue()).isEqualTo("secret");
        assertThat(directProps.getTruststoreTypeValue()).isEqualTo("PKCS12");
        assertThat(directProps.getClientCertPathValue()).isEqualTo("/cert/path");
        assertThat(directProps.getClientKeyPathValue()).isEqualTo("/key/path");
        assertThat(directProps.getCaCertPathValue()).isEqualTo("/ca/path");
    }
}
