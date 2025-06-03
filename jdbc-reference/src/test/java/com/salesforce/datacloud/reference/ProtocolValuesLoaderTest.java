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
import org.junit.jupiter.api.Test;

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
