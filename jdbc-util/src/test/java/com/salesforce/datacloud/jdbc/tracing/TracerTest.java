/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
