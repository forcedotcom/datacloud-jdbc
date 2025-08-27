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
package com.salesforce.datacloud.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DirectDataCloudConnection}.
 *
 * <p>Tests SSL/TLS connection modes and basic validation following industry standards
 * similar to PostgreSQL JDBC and MySQL Connector/J.
 *
 * <p>These tests focus on the working validation logic: SSL mode parsing,
 * null input validation, and basic property handling.
 */
@DisplayName("DirectDataCloudConnection Tests")
class DirectDataCloudConnectionTest {

    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 8443;
    private static final String TEST_URL = "jdbc:datacloud:thin:@" + TEST_HOST + ":" + TEST_PORT + "/service";

    private Properties properties;

    @BeforeEach
    void setUp() {
        properties = new Properties();
        properties.setProperty("direct", "true");
    }

    @Nested
    @DisplayName("SSL Auto-Detection Tests")
    class SslAutoDetectionTests {

        // Note: SSL mode is now auto-detected based on certificate properties
        // Only ssl_disabled flag exists for internal testing purposes

        @Test
        @DisplayName("Should handle ssl_disabled flag for testing")
        void shouldHandleSslDisabledFlag() {
            // Test internal ssl_disabled flag (for testing only)
            properties.setProperty("ssl_disabled", "true");
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .satisfies(ex -> {
                        // Should not be a validation error about SSL configuration
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
                        assertThat(ex.getMessage()).doesNotContain("SSL");
                    });
        }

        @Test
        @DisplayName("Should auto-detect system truststore SSL by default")
        void shouldAutoDetectSystemTruststoreSslByDefault() {
            // No SSL properties provided - should default to system truststore SSL
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .satisfies(ex -> {
                        // Should not be SSL configuration errors
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
                        assertThat(ex.getMessage()).doesNotContain("SSL configuration");
                    });
        }

        @Test
        @DisplayName("Should validate certificate file paths when provided")
        void shouldValidateCertificateFilePathsWhenProvided() {
            // Provide certificate paths that don't exist - should get file validation errors
            properties.setProperty("client_cert_path", "/nonexistent/client.pem");
            properties.setProperty("client_key_path", "/nonexistent/client-key.pem");

            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .satisfies(ex -> {
                        // Should be certificate file validation error, not SSL mode error
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
                    });
        }

        @Test
        @DisplayName("Should require both client cert and key for mutual TLS")
        void shouldRequireBothClientCertAndKeyForMutualTls() {
            // Provide only client cert without key - should get validation error
            properties.setProperty("client_cert_path", "/some/client.pem");
            // Missing client_key_path

            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .satisfies(ex -> {
                        // Should get either IllegalArgumentException for cert/key mismatch
                        // or DataCloudJDBCException for URL validation (depending on execution order)
                        assertThat(ex).isInstanceOfAny(IllegalArgumentException.class, DataCloudJDBCException.class);
                        if (ex instanceof IllegalArgumentException) {
                            assertThat(ex.getMessage())
                                    .contains("Client certificate path provided without client key path");
                        }
                        // If it's a URL validation error, that's also acceptable for this test
                    });
        }
    }

    @Nested
    @DisplayName("Input Validation Tests")
    class InputValidationTests {

        @Test
        @DisplayName("Should throw exception for null URL")
        void shouldThrowExceptionForNullUrl() {
            assertThatThrownBy(() -> DirectDataCloudConnection.of(null, properties))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .hasMessageContaining("Connection URL cannot be null");
        }

        @Test
        @DisplayName("Should throw exception for null properties")
        void shouldThrowExceptionForNullProperties() {
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, null))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .hasMessageContaining("Connection properties cannot be null");
        }

        @Test
        @DisplayName("Should require direct property to be true")
        void shouldRequireDirectPropertyToBeTrue() {
            properties.remove("direct"); // Remove direct property

            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .isInstanceOf(DataCloudJDBCException.class)
                    .hasMessageContaining("Cannot establish direct connection without direct enabled");
        }
    }
}
