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

import javax.annotation.concurrent.Immutable;
import lombok.experimental.UtilityClass;

@Immutable
@UtilityClass
public final class EncodingUtils {
    static final int BYTE_BASE16 = 2;
    private static final String ALPHABET = "0123456789abcdef";
    private static final char[] ENCODING = buildEncodingArray();
    private static final boolean[] VALID_HEX = buildValidHexArray();

    private static char[] buildEncodingArray() {
        char[] encoding = new char[512];
        for (int i = 0; i < 256; ++i) {
            encoding[i] = ALPHABET.charAt(i >>> 4);
            encoding[i | 0x100] = ALPHABET.charAt(i & 0xF);
        }
        return encoding;
    }

    private static boolean[] buildValidHexArray() {
        boolean[] validHex = new boolean[Character.MAX_VALUE];
        for (int i = 0; i < Character.MAX_VALUE; i++) {
            validHex[i] = (48 <= i && i <= 57) || (97 <= i && i <= 102);
        }
        return validHex;
    }

    /**
     * Appends the base16 encoding of the specified {@code value} to the {@code dest}.
     *
     * @param value the value to be converted.
     * @param dest the destination char array.
     * @param destOffset the starting offset in the destination char array.
     */
    public static void longToBase16String(long value, char[] dest, int destOffset) {
        byteToBase16((byte) (value >> 56 & 0xFFL), dest, destOffset);
        byteToBase16((byte) (value >> 48 & 0xFFL), dest, destOffset + BYTE_BASE16);
        byteToBase16((byte) (value >> 40 & 0xFFL), dest, destOffset + 2 * BYTE_BASE16);
        byteToBase16((byte) (value >> 32 & 0xFFL), dest, destOffset + 3 * BYTE_BASE16);
        byteToBase16((byte) (value >> 24 & 0xFFL), dest, destOffset + 4 * BYTE_BASE16);
        byteToBase16((byte) (value >> 16 & 0xFFL), dest, destOffset + 5 * BYTE_BASE16);
        byteToBase16((byte) (value >> 8 & 0xFFL), dest, destOffset + 6 * BYTE_BASE16);
        byteToBase16((byte) (value & 0xFFL), dest, destOffset + 7 * BYTE_BASE16);
    }

    /**
     * Encodes the specified byte, and returns the encoded {@code String}.
     *
     * @param value the value to be converted.
     * @param dest the destination char array.
     * @param destOffset the starting offset in the destination char array.
     */
    public static void byteToBase16(byte value, char[] dest, int destOffset) {
        int b = value & 0xFF;
        dest[destOffset] = ENCODING[b];
        dest[destOffset + 1] = ENCODING[b | 0x100];
    }

    /** Returns whether the {@link CharSequence} is a valid hex string. */
    public static boolean isValidBase16String(CharSequence value) {
        int len = value.length();
        for (int i = 0; i < len; i++) {
            char b = value.charAt(i);
            if (!isValidBase16Character(b)) {
                return false;
            }
        }
        return true;
    }

    /** Returns whether the given {@code char} is a valid hex character. */
    public static boolean isValidBase16Character(char b) {
        return VALID_HEX[b];
    }
}
