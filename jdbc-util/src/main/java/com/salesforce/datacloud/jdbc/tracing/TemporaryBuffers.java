/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.tracing;

public final class TemporaryBuffers {

    private static final ThreadLocal<char[]> CHAR_ARRAY = new ThreadLocal<>();

    private TemporaryBuffers() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * A {@link ThreadLocal} {@code char[]} of size {@code len}. Take care when using a large value of {@code len} as
     * this buffer will remain for the lifetime of the thread. The returned buffer will not be zeroed and may be larger
     * than the requested size, you must make sure to fill the entire content to the desired value and set the length
     * explicitly when converting to a {@link String}.
     */
    public static char[] chars(int len) {
        char[] buffer = CHAR_ARRAY.get();
        if (buffer == null || buffer.length < len) {
            buffer = new char[len];
            CHAR_ARRAY.set(buffer);
        }
        return buffer;
    }

    public static void clearChars() {
        CHAR_ARRAY.remove();
    }
}
