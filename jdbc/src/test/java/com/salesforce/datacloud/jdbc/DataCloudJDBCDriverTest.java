/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
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
        // A signed JWT with audienceTenantId=a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840
        final String fakeToken =
                "eyJraWQiOiJDT1JFLjAwRE9LMDAwMDAwOVp6ci4xNzE4MDUyMTU0NDIyIiwidHlwIjoiSldUIiwiYWxnIjoiRVMyNTYifQ.eyJzdWIiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS9pZC8wMERPSzAwMDAwMDlaenIyQUUvMDA1T0swMDAwMDBVeTkxWUFDIiwic2NwIjoiY2RwX3Byb2ZpbGVfYXBpIGNkcF9pbmdlc3RfYXBpIGNkcF9pZGVudGl0eXJlc29sdXRpb25fYXBpIGNkcF9zZWdtZW50X2FwaSBjZHBfcXVlcnlfYXBpIGNkcF9hcGkiLCJpc3MiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS8iLCJvcmdJZCI6IjAwRE9LMDAwMDAwOVp6ciIsImlzc3VlclRlbmFudElkIjoiY29yZS9mYWxjb250ZXN0MS1jb3JlNG9yYTE1LzAwRE9LMDAwMDAwOVp6cjJBRSIsInNmYXBwaWQiOiIzTVZHOVhOVDlUbEI3VmtZY0tIVm5sUUZzWEd6cUJuMGszUC5zNHJBU0I5V09oRU1OdkgyNzNpM1NFRzF2bWl3WF9YY2NXOUFZbHA3VnJnQ3BGb0ZXIiwiYXVkaWVuY2VUZW5hbnRJZCI6ImEzNjAvZmFsY29uZGV2L2E2ZDcyNmE3M2Y1MzQzMjdhNmE4ZTJlMGYzY2MzODQwIiwiY3VzdG9tX2F0dHJpYnV0ZXMiOnsiZGF0YXNwYWNlIjoiZGVmYXVsdCJ9LCJhdWQiOiJhcGkuYTM2MC5zYWxlc2ZvcmNlLmNvbSIsIm5iZiI6MTcyMDczMTAyMSwic2ZvaWQiOiIwMERPSzAwMDAwMDlaenIiLCJzZnVpZCI6IjAwNU9LMDAwMDAwVXk5MSIsImV4cCI6MTcyMDczODI4MCwiaWF0IjoxNzIwNzMxMDgxLCJqdGkiOiIwYjYwMzc4OS1jMGI2LTQwZTMtYmIzNi03NDQ3MzA2MzAxMzEifQ.lXgeAhJIiGoxgNpBi0W5oBWyn2_auB2bFxxajGuK6DMHlkqDhHJAlFN_uf6QPSjGSJCh5j42Ow5SrEptUDJwmQ";

        Properties properties = new Properties();
        properties.setProperty("cdpToken", fakeToken);
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
}
