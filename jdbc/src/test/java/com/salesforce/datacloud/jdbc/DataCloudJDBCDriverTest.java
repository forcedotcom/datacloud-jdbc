/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import java.nio.charset.StandardCharsets;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Base64;
import java.util.Properties;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class DataCloudJDBCDriverTest {
    public static final String VALID_URL = "jdbc:salesforce-datacloud://login.salesforce.com";

    @Test
    public void testIsDriverRegisteredInDriverManager() throws Exception {
        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");
        assertThat(DriverManager.getDriver(VALID_URL)).isNotNull().isInstanceOf(DataCloudJDBCDriver.class);
    }

    @Test
    public void testInvalidPrefixUrlNotAccepted() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();

        assertThat(driver.connect("jdbc:mysql://localhost:3306", properties)).isNull();
        assertThat(driver.acceptsURL("jdbc:mysql://localhost:3306")).isFalse();
    }

    @Test
    public void testValidUrlPrefixAccepted() throws Exception {
        final Driver driver = new DataCloudJDBCDriver();

        assertThat(driver.acceptsURL(VALID_URL)).isTrue();
    }

    @Test
    public void testSuccessfulDriverVersion() {
        assertThat(DriverVersion.getDriverName()).isEqualTo("salesforce-datacloud-jdbc");
        assertThat(DriverVersion.getProductName()).isEqualTo("salesforce-datacloud-queryservice");
        assertThat(DriverVersion.getProductVersion()).isEqualTo("1.0");

        final String version = DriverVersion.getDriverVersion();
        Pattern pattern = Pattern.compile("^\\d+(\\.\\d+)*(-SNAPSHOT)?$");
        assertThat(version)
                .isNotBlank()
                .matches(pattern)
                .as("We expect this string to start with a digit, if this fails make sure you've run mvn compile");

        final String expected = String.format("salesforce-datacloud-jdbc/%s", DriverVersion.getDriverVersion());
        assertThat(DriverVersion.formatDriverInfo()).isEqualTo(expected);
    }

    @Test
    public void testMissingClientId() {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();
        properties.setProperty("FOO", "BAR");

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> driver.connect(VALID_URL, properties))
                .withMessageContaining("Property `clientId` is missing");
    }

    @Test
    public void testUnknownPropertyRaisesUserError() {
        final Driver driver = new DataCloudJDBCDriver();
        Properties properties = new Properties();
        properties.setProperty("clientId", "123");
        properties.setProperty("clientSecret", "123");
        properties.setProperty("userName", "user");
        properties.setProperty("password", "pw");
        properties.setProperty("FOO", "BAR");

        assertThatExceptionOfType(SQLException.class)
                .isThrownBy(() -> driver.connect(VALID_URL, properties))
                .withMessageContaining("Unknown JDBC properties: FOO");
    }

    @Test
    public void testUnknownUrlParameterRaisesUserError() {
        final Driver driver = new DataCloudJDBCDriver();

        assertThatExceptionOfType(SQLException.class)
                .isThrownBy(() -> driver.connect(
                        VALID_URL + "?clientId=123&clientSecret=123&userName=user&password=pw&FOO=1234567890", null))
                .withMessageContaining("Unknown JDBC properties: FOO");
    }

    @Test
    public void testInvalidConnection() {
        // We expect that nobody is listening on port 23123
        String url = String.format("jdbc:salesforce-datacloud://localhost:23123");
        Properties properties = new Properties();
        properties.setProperty("clientId", "123");
        properties.setProperty("clientSecret", "123");
        properties.setProperty("userName", "user");
        properties.setProperty("password", "pw");
        // We expect that the connection will fail, so we set the max retries to 0 to make this test faster
        properties.setProperty("http.maxRetries", "0");

        assertThatThrownBy(() -> DriverManager.getConnection(url, properties))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Failed to connect to");
    }

    @Test
    public void testConnectUsingDirectCdpToken() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cdpToken", buildCdpJwt());
        properties.setProperty("tenantUrl", "test.c360a.salesforce.com");

        try (java.sql.Connection conn = DriverManager.getConnection(VALID_URL, properties)) {
            assertThat(conn).isNotNull();
            assertThat(conn.getMetaData().getUserName()).isEqualTo("");
        }
    }

    @Test
    public void testConnectUsingDirectCdpTokenRejectsInvalidJwt() {
        Properties properties = new Properties();
        properties.setProperty("cdpToken", "not-a-valid-jwt");
        properties.setProperty("tenantUrl", "test.c360a.salesforce.com");

        assertThatThrownBy(() -> DriverManager.getConnection(VALID_URL, properties))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Invalid CDP token");
    }

    private static String buildCdpJwt() {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = "{\"alg\":\"ES256\",\"typ\":\"JWT\"}";
        String payload = "{\"audienceTenantId\":\"a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840\",\"exp\":"
                + (Instant.now().getEpochSecond() + 3600) + "}";
        String h = enc.encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String p = enc.encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return h + "." + p + ".sig";
    }
}
