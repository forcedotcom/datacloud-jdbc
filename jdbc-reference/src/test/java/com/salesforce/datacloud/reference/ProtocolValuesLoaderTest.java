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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to ensure that the protocol values file can be deserialized.
 */
class ProtocolValuesLoaderTest {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolValuesLoaderTest.class);

    @Test
    void testLoadProtocolValues() throws IOException {
        List<ProtocolValue> protocolValues = ProtocolValue.loadProtocolValues();
        assertNotNull(protocolValues);
        assertFalse(protocolValues.isEmpty());

        // Verify that we have some expected data
        assertTrue(protocolValues.size() > 100); // Should have many test cases

        logger.info("✅ Successfully loaded {} protocol values", protocolValues.size());
    }

    @Test
    void testProtocolValuesContainTimestampTypes() throws IOException {
        List<ProtocolValue> protocolValues = ProtocolValue.loadProtocolValues();

        // Find timestamp-related protocol values
        Optional<ProtocolValue> timestampValue = protocolValues.stream()
                .filter(pv -> pv.getSql().contains("timestamp"))
                .findFirst();

        assertTrue(timestampValue.isPresent(), "Protocol values should contain timestamp types for timezone testing");

        ProtocolValue value = timestampValue.get();
        assertNotNull(value.getSql(), "Timestamp protocol value should have SQL");
        assertTrue(value.getSql().contains("timestamp"), "SQL should contain timestamp reference");

        logger.info("✅ Found timestamp protocol value: {}", value.getSql());
    }

    @Test
    void testProtocolValuesContainTimestamptzTypes() throws IOException {
        List<ProtocolValue> protocolValues = ProtocolValue.loadProtocolValues();

        // Find timestamptz-related protocol values
        Optional<ProtocolValue> timestamptzValue = protocolValues.stream()
                .filter(pv -> pv.getSql().contains("timestamptz"))
                .findFirst();

        assertTrue(
                timestamptzValue.isPresent(), "Protocol values should contain timestamptz types for timezone testing");

        ProtocolValue value = timestamptzValue.get();
        assertNotNull(value.getSql(), "Timestamptz protocol value should have SQL");
        assertTrue(value.getSql().contains("timestamptz"), "SQL should contain timestamptz reference");

        logger.info("✅ Found timestamptz protocol value: {}", value.getSql());
    }

    @Test
    void testProtocolValuesStructure() throws IOException {
        List<ProtocolValue> protocolValues = ProtocolValue.loadProtocolValues();

        // Verify basic structure of protocol values
        long nullValues = protocolValues.stream()
                .filter(pv -> pv.getInterestingness() == ProtocolValue.Interestingness.Null)
                .count();

        long defaultValues = protocolValues.stream()
                .filter(pv -> pv.getInterestingness() == ProtocolValue.Interestingness.Default)
                .count();

        assertTrue(nullValues > 0, "Should have NULL test cases");
        assertTrue(defaultValues > 0, "Should have DEFAULT test cases");

        logger.info("✅ Protocol values structure: {} null values, {} default values", nullValues, defaultValues);
    }
}
