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
package com.salesforce.datacloud.jdbc.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class Constants {

    public static final String LOGIN_URL = "loginURL";

    // Property constants
    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String USER = "user";
    public static final String USER_NAME = "userName";
    public static final String PRIVATE_KEY = "privateKey";
    public static final String FORCE_SYNC = "force-sync";

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
}
