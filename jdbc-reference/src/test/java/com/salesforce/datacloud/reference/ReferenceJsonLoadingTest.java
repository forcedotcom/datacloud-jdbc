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
