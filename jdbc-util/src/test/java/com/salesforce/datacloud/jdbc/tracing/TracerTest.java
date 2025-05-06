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
package com.salesforce.datacloud.jdbc.tracing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TracerTest {

    @Test
    public void testValidTraceId() {
        String validTraceId = Tracer.get().nextId();
        Assertions.assertTrue(Tracer.get().isValidTraceId(validTraceId));
    }

    @Test
    public void testValidSpanId() {
        String validSpanId = Tracer.get().nextSpanId();
        Assertions.assertTrue(Tracer.get().isValidSpanId(validSpanId));
    }

    @Test
    public void testInvalidTraceIdNull() {
        Assertions.assertFalse(Tracer.get().isValidTraceId(null));
    }

    @Test
    public void testInvalidTraceIdTooShort() {
        String invalidTraceId = "abc";
        Assertions.assertFalse(Tracer.get().isValidTraceId(invalidTraceId));
    }

    @Test
    public void testInvalidTraceIdTooLong() {
        String invalidTraceId = "463ac35c9f6413ad48485a3953bb61240000000000000000";
        Assertions.assertFalse(Tracer.get().isValidTraceId(invalidTraceId));
    }

    @Test
    public void testInvalidTraceIdNonHexCharacters() {
        String invalidTraceId = "463ac35c9f6413ad48485a3953bb612g";
        Assertions.assertFalse(Tracer.get().isValidTraceId(invalidTraceId));
    }

    @Test
    public void testNextId() {
        String traceId = Tracer.get().nextId();

        Assertions.assertNotNull(traceId);
        Assertions.assertFalse(traceId.isEmpty());
    }

    @Test
    public void testNextSpanId() {
        String spanId = Tracer.get().nextSpanId();

        Assertions.assertNotNull(spanId);
        Assertions.assertFalse(spanId.isEmpty());
    }
}
