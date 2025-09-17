/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

public final class Constants {
    // Column Types
    public static final String INTEGER = "INTEGER";
    public static final String TEXT = "TEXT";
    public static final String SHORT = "SHORT";

    // Date Time constants
    public static final String ISO_TIME_FORMAT = "HH:mm:ss";

    private Constants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
