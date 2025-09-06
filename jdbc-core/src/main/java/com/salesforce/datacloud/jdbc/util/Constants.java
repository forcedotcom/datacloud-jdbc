/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.util;

public final class Constants {
    public static final String LOGIN_URL = "loginURL";

    // Property constants
    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String USER = "user";
    public static final String USER_NAME = "userName";
    public static final String PRIVATE_KEY = "privateKey";

    // Column Types
    public static final String INTEGER = "INTEGER";
    public static final String TEXT = "TEXT";
    public static final String SHORT = "SHORT";

    public static final String DRIVER_NAME = "salesforce-datacloud-jdbc";
    public static final String DATABASE_PRODUCT_NAME = "salesforce-datacloud-queryservice";
    public static final String DATABASE_PRODUCT_VERSION = "24.8.0";
    public static final String DRIVER_VERSION = "3.0";

    // Date Time constants

    public static final String ISO_TIME_FORMAT = "HH:mm:ss";

    private Constants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
