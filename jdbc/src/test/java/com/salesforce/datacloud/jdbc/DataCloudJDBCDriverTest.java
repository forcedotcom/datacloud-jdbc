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

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import com.salesforce.datacloud.jdbc.core.DataCloudConnectionString;
import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.fail;


public class DataCloudJDBCDriverTest {
    public static final String VALID_URL = "jdbc:salesforce-datacloud://login.salesforce.com";
    private static final String DRIVER_NAME = "salesforce-datacloud-jdbc";
    private static final String PRODUCT_NAME = "salesforce-datacloud-queryservice";
    private static final String PRODUCT_VERSION = "1.0";
    private final Pattern pattern = Pattern.compile("^\\d+(\\.\\d+)*(-SNAPSHOT)?$");

    @Test
    public void testIsDriverRegisteredInDriverManager() throws Exception {
        assertThat(DriverManager.getDriver(VALID_URL)).isNotNull().isInstanceOf(DataCloudJDBCDriver.class);
    }

    @Test
    public void  testNullUrlNotAllowedWhenConnecting() {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();

        assertThatExceptionOfType(SQLException.class).isThrownBy(() -> driver.connect(null, properties));
    }

    @Test
    public void  testUnsupportedPrefixUrlNotAllowedWhenConnecting() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();

        assertThat(driver.connect("jdbc:mysql://localhost:3306", properties)).isNull();
    }

    @Test
    public void  testInvalidPrefixUrlNotAccepted() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();

        assertThat(driver.acceptsURL("jdbc:mysql://localhost:3306")).isFalse();
    }

    @Test
    public void  testGetMajorVersion() {
        final Driver driver = new DataCloudJDBCDriver();
        assertThat(driver.getMajorVersion()).isEqualTo(DriverVersion.getMajorVersion());
    }

    @Test
    public void  testGetMinorVersion() {
        final Driver driver = new DataCloudJDBCDriver();
        assertThat(driver.getMinorVersion()).isEqualTo(DriverVersion.getMinorVersion());
    }

    @Test
    public void  testValidUrlPrefixAccepted() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();

        assertThat(driver.acceptsURL(VALID_URL)).isTrue();
    }

    @Test
    public void  testjdbcCompliant() {
        final Driver driver = new DataCloudJDBCDriver();
        assertThat(driver.jdbcCompliant()).isFalse();
    }

    @Test
    public void  testSuccessfulDriverVersion() {
        assertThat(DriverVersion.getDriverName()).isEqualTo(DRIVER_NAME);
        assertThat(DriverVersion.getProductName()).isEqualTo(PRODUCT_NAME);
        assertThat(DriverVersion.getProductVersion()).isEqualTo(PRODUCT_VERSION);

        final String version = DriverVersion.getDriverVersion();
        assertThat(version)
                .isNotBlank()
                .matches(pattern)
                .as("We expect this string to start with a digit, if this fails make sure you've run mvn compile");

        final String expected = String.format("%s/%s", DRIVER_NAME, DriverVersion.getDriverVersion());
        assertThat(DriverVersion.formatDriverInfo()).isEqualTo(expected);
    }

    @Test
    public void testGetURL() throws SQLException {
        final String url = "jdbc:salesforce-datacloud://login.salesforce.com";

        final Driver driver;
        try {
            driver = DriverManager.getDriver(url);
        } catch (Exception e) {
            fail(e);
            throw e;
        }

        assertThat(url).isEqualTo(DataCloudConnectionString.CONNECTION_PROTOCOL + "//login.salesforce.com");
        assertThat(driver).isInstanceOf(DataCloudJDBCDriver.class);
    }
}
