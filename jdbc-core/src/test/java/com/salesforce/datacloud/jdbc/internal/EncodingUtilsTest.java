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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EncodingUtilsTest {
    private static final long FIRST_LONG = 0x1213141516171819L;
    private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
    private static final int BYTE_BASE16 = 2;
    private static final int LONG_BASE16 = BYTE_BASE16 * LONG_BYTES;

    private static final char[] FIRST_CHAR_ARRAY =
            new char[] {'1', '2', '1', '3', '1', '4', '1', '5', '1', '6', '1', '7', '1', '8', '1', '9'};
    private static final long SECOND_LONG = 0xFFEEDDCCBBAA9988L;
    private static final char[] SECOND_CHAR_ARRAY =
            new char[] {'f', 'f', 'e', 'e', 'd', 'd', 'c', 'c', 'b', 'b', 'a', 'a', '9', '9', '8', '8'};
    private static final char[] BOTH_CHAR_ARRAY = new char[] {
        '1', '2', '1', '3', '1', '4', '1', '5', '1', '6', '1', '7', '1', '8', '1', '9', 'f', 'f', 'e', 'e', 'd', 'd',
        'c', 'c', 'b', 'b', 'a', 'a', '9', '9', '8', '8'
    };

    @Test
    void testLongToBase16String() {
        char[] chars1 = new char[LONG_BASE16];
        EncodingUtils.longToBase16String(FIRST_LONG, chars1, 0);
        assertThat(chars1).isEqualTo(FIRST_CHAR_ARRAY);

        char[] chars2 = new char[LONG_BASE16];
        EncodingUtils.longToBase16String(SECOND_LONG, chars2, 0);
        assertThat(chars2).isEqualTo(SECOND_CHAR_ARRAY);

        char[] chars3 = new char[2 * LONG_BASE16];
        EncodingUtils.longToBase16String(FIRST_LONG, chars3, 0);
        EncodingUtils.longToBase16String(SECOND_LONG, chars3, LONG_BASE16);
        assertThat(chars3).isEqualTo(BOTH_CHAR_ARRAY);
    }

    @Test
    void testValidHex() {
        assertThat(EncodingUtils.isValidBase16String("abcdef1234567890")).isTrue();
        assertThat(EncodingUtils.isValidBase16String("abcdefg1234567890")).isFalse();
        assertThat(EncodingUtils.isValidBase16String("<abcdef1234567890")).isFalse();
        assertThat(EncodingUtils.isValidBase16String("(abcdef1234567890")).isFalse();
        assertThat(EncodingUtils.isValidBase16String("abcdef1234567890B")).isFalse();
    }
}
