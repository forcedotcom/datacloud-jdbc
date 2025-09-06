/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.reference;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;
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
}
