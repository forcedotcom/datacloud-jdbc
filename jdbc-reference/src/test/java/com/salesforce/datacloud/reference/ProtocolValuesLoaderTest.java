package com.salesforce.datacloud.reference;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class demonstrating how to load protocol values.
 */
class ProtocolValuesLoaderTest {

    @Test
    void testLoadProtocolValues() throws IOException {
        List<ProtocolValue> protocolValues = ProtocolValue.loadProtocolValues();
        assertNotNull(protocolValues);
        assertFalse(protocolValues.isEmpty());

        // Verify that we have some expected data
        assertTrue(protocolValues.size() > 100); // Should have many test cases
    }
}