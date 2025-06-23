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
package com.salesforce.datacloud.jdbc.config;

import java.util.Properties;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.val;

public final class DriverVersion {
    private static final String DRIVER_NAME = "salesforce-datacloud-jdbc";
    private static final String DATABASE_PRODUCT_NAME = "salesforce-datacloud-queryservice";
    private static final String DATABASE_PRODUCT_VERSION = "1.0";

    @Getter(lazy = true)
    private static final DriverVersionInfo driverVersionInfo = DriverVersionInfo.of();

    private DriverVersion() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static int getMajorVersion() {
        return getDriverVersionInfo().getMajor();
    }

    public static int getMinorVersion() {
        return getDriverVersionInfo().getMinor();
    }

    public static String getDriverName() {
        return DRIVER_NAME;
    }

    public static String getProductName() {
        return DATABASE_PRODUCT_NAME;
    }

    public static String getProductVersion() {
        return DATABASE_PRODUCT_VERSION;
    }

    public static String formatDriverInfo() {
        return String.format("%s/%s", getDriverName(), getDriverVersionInfo());
    }

    public static String getDriverVersion() {
        return getDriverVersionInfo().toString();
    }
}

@Value
@Builder(access = AccessLevel.PRIVATE)
class DriverVersionInfo {
    @Builder.Default
    int major = 0;

    @Builder.Default
    int minor = 0;

    static String getVersion(Properties properties) {
        String version = properties.getProperty("version");
        if (version == null || version.isEmpty()) {
            return "0.0";
        }
        return version.replaceAll("-.*$", "");
    }

    static DriverVersionInfo of(Properties properties) {
        val builder = DriverVersionInfo.builder();
        String version = getVersion(properties);
        if (!version.isEmpty()) {
            val chunks = version.split("\\.", -1);
            builder.major(Integer.parseInt(chunks[0]));
            builder.minor(Integer.parseInt(chunks[1]));
            return builder.build();
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return String.format("%d.%d", major, minor);
    }

    static DriverVersionInfo of() {
        val properties = ResourceReader.readResourceAsProperties("/driver-version.properties");
        return of(properties);
    }
}
