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
package com.salesforce.datacloud.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DataCloudJDBCDriverTest {
    public static final String VALID_URL = "jdbc:salesforce-datacloud://login.salesforce.com";
    private static final String DRIVER_NAME = "salesforce-datacloud-jdbc";
    private static final String PRODUCT_NAME = "salesforce-datacloud-queryservice";
    private static final String PRODUCT_VERSION = "1.0";
    private final Pattern pattern = Pattern.compile("^\\d+(\\.\\d+)*(-SNAPSHOT)?$");

    @Test
    void testIsDriverRegisteredInDriverManager() throws Exception {
        assertThat(DriverManager.getDriver(VALID_URL)).isNotNull().isInstanceOf(DataCloudJDBCDriver.class);
    }

    @Test
    void testNullUrlNotAllowedWhenConnecting() {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();

        assertThatExceptionOfType(SQLException.class).isThrownBy(() -> driver.connect(null, properties));
    }

    @Test
    void testUnsupportedPrefixUrlNotAllowedWhenConnecting() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();

        assertThat(driver.connect("jdbc:mysql://localhost:3306", properties)).isNull();
    }

    @Test
    void testInvalidPrefixUrlNotAccepted() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();

        assertThat(driver.acceptsURL("jdbc:mysql://localhost:3306")).isFalse();
    }

    @Test
    void testGetMajorVersion() {
        final Driver driver = new DataCloudJDBCDriver();
        assertThat(driver.getMajorVersion()).isEqualTo(DriverVersion.getMajorVersion());
    }

    @Test
    void testGetMinorVersion() {
        final Driver driver = new DataCloudJDBCDriver();
        assertThat(driver.getMinorVersion()).isEqualTo(DriverVersion.getMinorVersion());
    }

    @Test
    void testValidUrlPrefixAccepted() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();

        assertThat(driver.acceptsURL(VALID_URL)).isTrue();
    }

    @Test
    void testjdbcCompliant() {
        final Driver driver = new DataCloudJDBCDriver();
        assertThat(driver.jdbcCompliant()).isFalse();
    }

    @Test
    void testSuccessfulDriverVersion() {
        Assertions.assertEquals(DRIVER_NAME, DriverVersion.getDriverName());
        Assertions.assertEquals(PRODUCT_NAME, DriverVersion.getProductName());
        Assertions.assertEquals(PRODUCT_VERSION, DriverVersion.getProductVersion());

        val version = DriverVersion.getDriverVersion();
        assertThat(version)
                .isNotBlank()
                .matches(pattern)
                .as("We expect this string to start with a digit, if this fails make sure you've run mvn compile");

        val formattedDriverInfo = DriverVersion.formatDriverInfo();
        Assertions.assertEquals(
                String.format("%s/%s", DRIVER_NAME, DriverVersion.getDriverVersion()), formattedDriverInfo);
    }
}
