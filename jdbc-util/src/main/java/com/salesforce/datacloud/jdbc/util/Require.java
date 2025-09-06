/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

public final class Require {
    private Require() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static void requireNotNullOrBlank(String value, String name) {
        if (StringCompatibility.isNullOrBlank(value)) {
            throw new IllegalArgumentException("Expected argument '" + name + "' to not be null or blank");
        }
    }
}
