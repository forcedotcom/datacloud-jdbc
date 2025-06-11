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

import java.util.Arrays;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Represents a value along with its Java class name.
 * Used for persisting both the string representation and the Java type information.
 */
@Data
@Builder
@Jacksonized
public class ValueWithClass {
    /**
     * The string representation of the value (null if the value is null)
     */
    private String stringValue;

    /**
     * The Java class name of the original object (null if the value is null)
     */
    private String javaClassName;

    /**
     * Creates a ValueWithClass from an object.
     *
     * @param value the object to create ValueWithClass from
     * @return ValueWithClass containing the string representation and class name
     */
    public static ValueWithClass from(Object value) {
        if (value == null) {
            return ValueWithClass.builder()
                    .stringValue(null)
                    .javaClassName(null)
                    .build();
        }

        String stringValue = convertToMeaningfulString(value);

        return ValueWithClass.builder()
                .stringValue(stringValue)
                .javaClassName(value.getClass().getName())
                .build();
    }

    /**
     * Converts an object to a meaningful string representation.
     * Handles special cases where toString() doesn't provide useful output.
     *
     * @param value the object to convert
     * @return meaningful string representation
     */
    private static String convertToMeaningfulString(Object value) {
        if (value == null) {
            return null;
        }

        // Handle byte arrays specially
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            StringBuilder hex = new StringBuilder();
            for (byte b : bytes) {
                hex.append(String.format("%02x", b));
            }
            return "\\x" + hex.toString();
        }

        // Handle other array types
        if (value.getClass().isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType.isPrimitive()) {
                // Handle primitive arrays
                if (componentType == int.class) {
                    return Arrays.toString((int[]) value);
                } else if (componentType == long.class) {
                    return Arrays.toString((long[]) value);
                } else if (componentType == double.class) {
                    return Arrays.toString((double[]) value);
                } else if (componentType == float.class) {
                    return Arrays.toString((float[]) value);
                } else if (componentType == boolean.class) {
                    return Arrays.toString((boolean[]) value);
                } else if (componentType == char.class) {
                    return Arrays.toString((char[]) value);
                } else if (componentType == short.class) {
                    return Arrays.toString((short[]) value);
                }
            } else {
                // Handle object arrays
                return Arrays.toString((Object[]) value);
            }
        }

        // For all other types, use toString()
        return value.toString();
    }
}
