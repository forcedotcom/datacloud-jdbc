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
package com.salesforce.datacloud.jdbc.internal;

public class Tracer {
    private static final int TRACE_ID_BYTES_LENGTH = 16;
    private static final int TRACE_ID_HEX_LENGTH = 2 * TRACE_ID_BYTES_LENGTH;
    private static final int SPAN_ID_BYTES_LENGTH = 8;
    private static final int SPAN_ID_HEX_LENGTH = 2 * SPAN_ID_BYTES_LENGTH;
    private static final long INVALID_ID = 0;

    private static final String INVALID = "0000000000000000";

    private static volatile Tracer instance;

    public static synchronized Tracer get() {
        if (instance == null) {
            synchronized (Tracer.class) {
                if (instance == null) {
                    instance = new Tracer();
                }
            }
        }
        return instance;
    }

    public String nextSpanId() {
        long id;
        do {
            id = randomLong();
        } while (id == INVALID_ID);
        return fromLong(id);
    }

    public boolean isValidSpanId(CharSequence spanId) {
        return spanId != null
                && spanId.length() == SPAN_ID_HEX_LENGTH
                && !INVALID.contentEquals(spanId)
                && EncodingUtils.isValidBase16String(spanId);
    }

    private String fromLong(long id) {
        if (id == 0) {
            return INVALID;
        }
        char[] result = TemporaryBuffers.chars(SPAN_ID_HEX_LENGTH);
        EncodingUtils.longToBase16String(id, result, 0);
        return new String(result, 0, SPAN_ID_HEX_LENGTH);
    }

    private long randomLong() {
        return java.util.concurrent.ThreadLocalRandom.current().nextLong();
    }

    public String nextId() {
        long idHi = randomLong();
        long idLo;
        do {
            idLo = randomLong();
        } while (idLo == 0);
        return fromLongs(idHi, idLo);
    }

    public boolean isValidTraceId(CharSequence traceId) {
        return traceId != null
                && traceId.length() == TRACE_ID_HEX_LENGTH
                && !INVALID.contentEquals(traceId)
                && EncodingUtils.isValidBase16String(traceId);
    }

    private String fromLongs(long traceIdLongHighPart, long traceIdLongLowPart) {
        if (traceIdLongHighPart == 0 && traceIdLongLowPart == 0) {
            return INVALID;
        }
        char[] chars = TemporaryBuffers.chars(TRACE_ID_HEX_LENGTH);
        EncodingUtils.longToBase16String(traceIdLongHighPart, chars, 0);
        EncodingUtils.longToBase16String(traceIdLongLowPart, chars, 16);
        return new String(chars, 0, TRACE_ID_HEX_LENGTH);
    }
}
