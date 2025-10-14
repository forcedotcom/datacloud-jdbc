/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class HyperJDBCDriverTest {
    public static final String FAKE_URL = "jdbc:salesforce-hyper://login.salesforce.com";

    @Test
    public void testIsDriverRegisteredInDriverManager() throws Exception {
        Class.forName("com.salesforce.datacloud.jdbc.HyperJDBCDriver");
        assertThat(DriverManager.getDriver(FAKE_URL)).isNotNull().isInstanceOf(HyperJDBCDriver.class);
    }

    @Test
    public void testSuccessfulConnection() throws SQLException {
        int port = HyperServerManager.get(ConfigFile.SMALL_CHUNKS).getPort();
        Properties properties = new Properties();
        // Test a couple of properties
        properties.setProperty("grpc.keepAlive", "false");
        properties.setProperty("workload", "test-workload");

        String url = String.format("jdbc:salesforce-hyper://localhost:%d", port);
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT 1");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getInt(1)).isEqualTo(1);
            }
        }
    }

    @Test
    public void testUrlParameters() throws SQLException {
        int port = HyperServerManager.get(ConfigFile.SMALL_CHUNKS).getPort();
        String url = String.format("jdbc:salesforce-hyper://localhost:%d?workload=test-workload", port);
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT 1");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getInt(1)).isEqualTo(1);
            }
        }
    }

    @Test
    public void testInvalidPrefixUrlNotAccepted() throws Exception {
        final Driver driver = new HyperJDBCDriver();
        Properties properties = new Properties();

        assertThat(driver.connect("jdbc:mysql://localhost:3306", properties)).isNull();
        assertThat(driver.acceptsURL("jdbc:mysql://localhost:3306")).isFalse();
    }

    @Test
    public void testValidUrlPrefixAccepted() throws Exception {
        final Driver driver = new HyperJDBCDriver();

        assertThat(driver.acceptsURL(FAKE_URL)).isTrue();
    }

    @Test
    public void testUnknownPropertyRaisesUserError() {
        final Driver driver = new HyperJDBCDriver();
        Properties properties = new Properties();
        // Note that `clientId` is accepted by the DataCloud driver, but not by the Hyper driver
        properties.setProperty("clientId", "1234567890");

        assertThatExceptionOfType(SQLException.class)
                .isThrownBy(() -> driver.connect(FAKE_URL, properties))
                .withMessageContaining("Unknown JDBC properties: clientId");
    }

    @Test
    public void testUnknownUrlParameterRaisesUserError() {
        final Driver driver = new HyperJDBCDriver();

        assertThatExceptionOfType(SQLException.class)
                .isThrownBy(() -> driver.connect(FAKE_URL + "?clientId=1234567890", null))
                .withMessageContaining("Unknown JDBC properties: clientId");
    }

    @Test
    public void testInvalidConnection() {
        // We expect that nobody is listening on port 23123
        String url = String.format("jdbc:salesforce-hyper://localhost:23123");

        assertThatThrownBy(() -> {
                    try (Connection connection = DriverManager.getConnection(url, null)) {
                        try (Statement statement = connection.createStatement()) {
                            // Execute an actual query to trigger the connection creation
                            statement.executeQuery("SELECT 1");
                        }
                    }
                })
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Failed to execute query");
    }
}
