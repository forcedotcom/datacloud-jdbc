/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.reference;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test class to ensure that the protocol values file can be deserialized.
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
