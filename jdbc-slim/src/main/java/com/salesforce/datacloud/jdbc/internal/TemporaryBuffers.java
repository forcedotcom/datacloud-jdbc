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

import lombok.experimental.UtilityClass;

@UtilityClass
public final class TemporaryBuffers {

    private static final ThreadLocal<char[]> CHAR_ARRAY = new ThreadLocal<>();

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
