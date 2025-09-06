/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

public final class SqlErrorCodes {
    public static final String FEATURE_NOT_SUPPORTED = "0A000";
    public static final String UNDEFINED_FILE = "58P01";

    private SqlErrorCodes() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
