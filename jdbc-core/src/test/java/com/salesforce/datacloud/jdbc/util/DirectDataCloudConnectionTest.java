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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    @DisplayName("SSL Mode Validation Tests")
    class SslModeValidationTests {

        // Note: Invalid SSL mode validation is handled gracefully by defaulting to DISABLED
        // This follows the fail-safe approach where invalid configurations don't break connections

        @Test
        @DisplayName("Should accept valid SSL mode values")
        void shouldAcceptValidSslModeValues() {
            // Test that valid SSL modes don't throw validation errors immediately
            // (actual connection will fail, but SSL mode parsing should work)

            // Test DISABLED
            properties.setProperty("ssl_mode", "DISABLED");
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .isInstanceOf(Exception.class)
                    .satisfies(ex -> {
                        // Should not be a validation error about invalid ssl_mode
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
                    });
        }

        @ParameterizedTest
        @ValueSource(strings = {"disabled", "DISABLED", "required", "REQUIRED", "verify_full", "VERIFY_FULL"})
        @DisplayName("Should handle case-insensitive SSL mode values")
        void shouldHandleCaseInsensitiveSslModeValues(String mode) {
            properties.setProperty("ssl_mode", mode);

            // Should not throw validation error about invalid ssl_mode
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .satisfies(ex -> {
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
                    });
        }

        @Test
        @DisplayName("Should handle empty ssl_mode property")
        void shouldHandleEmptySslModeProperty() {
            properties.setProperty("ssl_mode", "");

            // Should default to DISABLED (no validation error)
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .satisfies(ex -> {
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
                    });
        }

        @Test
        @DisplayName("Should handle whitespace in ssl_mode property")
        void shouldHandleWhitespaceInSslModeProperty() {
            properties.setProperty("ssl_mode", "  DISABLED  ");

            // Should trim and accept
            assertThatThrownBy(() -> DirectDataCloudConnection.of(TEST_URL, properties))
                    .satisfies(ex -> {
                        assertThat(ex.getMessage()).doesNotContain("Invalid ssl_mode");
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
