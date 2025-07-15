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
package com.salesforce.datacloud.reference;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for loading and validating the reference.json file.
 * These tests verify that the baseline file can be loaded and parsed correctly
 * without requiring any external dependencies like PostgreSQL.
 */
public class ReferenceJsonLoadingTest {

    private static final Logger logger = LoggerFactory.getLogger(ReferenceJsonLoadingTest.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Test that verifies the reference.json file exists and can be loaded as a resource.
     */
    @Test
    @SneakyThrows
    void testReferenceJsonFileExists() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("reference.json")) {
            assertNotNull(inputStream, "reference.json should exist in resources directory");
        }
    }

    /**
     * Test that verifies the reference.json file contains valid JSON that can be parsed.
     */
    @Test
    @SneakyThrows
    void testReferenceJsonValidFormat() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("reference.json")) {
            assertNotNull(inputStream, "reference.json should exist");

            // Test that we can parse the JSON successfully
            List<ReferenceEntry> entries =
                    objectMapper.readValue(inputStream, new TypeReference<List<ReferenceEntry>>() {});

            assertNotNull(entries, "Parsed reference entries should not be null");
            assertFalse(entries.isEmpty(), "Reference entries should not be empty");

            logger.info("✅ Successfully parsed reference.json with {} entries", entries.size());
        }
    }

    /**
     * Test that verifies the reference.json file contains timestamp entries for timezone validation.
     */
    @Test
    @SneakyThrows
    void testReferenceJsonContainsTimestampEntries() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("reference.json")) {
            assertNotNull(inputStream, "reference.json should exist");

            List<ReferenceEntry> entries =
                    objectMapper.readValue(inputStream, new TypeReference<List<ReferenceEntry>>() {});

            // Find timestamp-related entries
            Optional<ReferenceEntry> timestampEntry = entries.stream()
                    .filter(entry -> entry.getQuery().contains("timestamp"))
                    .findFirst();

            assertTrue(
                    timestampEntry.isPresent(), "Reference should contain timestamp entries for timezone validation");

            ReferenceEntry entry = timestampEntry.get();
            assertNotNull(entry.getColumnMetadata(), "Timestamp entry should have column metadata");
            assertFalse(entry.getColumnMetadata().isEmpty(), "Timestamp entry should have at least one column");

            logger.info("✅ Found timestamp entry: {}", entry.getQuery());
        }
    }

    /**
     * Test timezone configuration methods from PostgresReferenceGenerator.
     */
    @Test
    void testTimezoneConfiguration() {
        // Test default timezone
        String defaultTimezone = PostgresReferenceGenerator.getSessionTimezone();
        assertEquals("America/Los_Angeles", defaultTimezone, "Default timezone should be America/Los_Angeles");

        // Test system property override
        System.setProperty(PostgresReferenceGenerator.SESSION_TIMEZONE_PROPERTY, "America/New_York");
        try {
            String systemTimezone = PostgresReferenceGenerator.getSessionTimezone();
            assertEquals("America/New_York", systemTimezone, "System property should override default timezone");
        } finally {
            System.clearProperty(PostgresReferenceGenerator.SESSION_TIMEZONE_PROPERTY);
        }
    }

    /**
     * Test connection properties include timezone configuration.
     */
    @Test
    void testConnectionPropertiesIncludeTimezone() {
        Properties properties = PostgresReferenceGenerator.createConnectionProperties();

        // Verify connection properties are set
        assertEquals(PostgresReferenceGenerator.DB_USER, properties.getProperty("user"));
        assertEquals(PostgresReferenceGenerator.DB_PASSWORD, properties.getProperty("password"));

        // Verify timezone properties are set
        String timezone = properties.getProperty("timezone");
        assertNotNull(timezone, "Connection properties should include timezone");
        assertEquals("America/Los_Angeles", timezone, "Default timezone should be America/Los_Angeles");

        String querySettingTimezone = properties.getProperty("querySetting.timezone");
        assertNotNull(querySettingTimezone, "Connection properties should include querySetting.timezone");
        assertEquals(timezone, querySettingTimezone, "querySetting.timezone should match timezone");

        logger.info("✅ Connection properties include timezone: {}", timezone);
    }

    /**
     * Test timezone configuration with custom system property.
     */
    @Test
    void testTimezoneConfigurationWithCustomSystemProperty() {
        String customTimezone = "Europe/London";
        System.setProperty(PostgresReferenceGenerator.SESSION_TIMEZONE_PROPERTY, customTimezone);

        try {
            Properties properties = PostgresReferenceGenerator.createConnectionProperties();

            String timezone = properties.getProperty("timezone");
            assertEquals(customTimezone, timezone, "Custom timezone should be applied");

            String querySettingTimezone = properties.getProperty("querySetting.timezone");
            assertEquals(
                    customTimezone, querySettingTimezone, "Custom timezone should be applied to querySetting.timezone");

            logger.info("✅ Custom timezone configuration works: {}", customTimezone);
        } finally {
            System.clearProperty(PostgresReferenceGenerator.SESSION_TIMEZONE_PROPERTY);
        }
    }
}
