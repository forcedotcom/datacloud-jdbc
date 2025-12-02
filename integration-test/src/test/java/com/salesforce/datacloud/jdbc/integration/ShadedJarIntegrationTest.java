/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Integration test that validates the shaded JDBC JAR works correctly.
 * This test focuses on the critical functionality that was broken by the service file regression:
 * - Driver loading and registration
 * - Connection establishment
 * - Basic query execution
 *
 * Note: This test requires JVM arguments for Apache Arrow memory access on Java 9+:
 * --add-opens=java.base/java.nio=com.salesforce.datacloud.shaded.org.apache.arrow.memory.core,ALL-UNNAMED
 * (These are automatically added when using the runIntegrationTest Gradle task for Java 9+)
 * Java 8 doesn't need these arguments as it doesn't have the module system.
 */
@Slf4j
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ShadedJarIntegrationTest {

    private static final String DRIVER_CLASS = "com.salesforce.datacloud.jdbc.DataCloudJDBCDriver";

    @BeforeAll
    static void setUp() {
        log.info("Starting Shaded JAR Integration Test Suite");
    }

    /**
     * Test 1: Verify the JDBC driver can be loaded from the shaded JAR
     */
    @Test
    @Order(1)
    void testDriverLoading() throws Exception {
        log.info("Test 1: Driver Loading");

        Class<?> driverClass = Class.forName(DRIVER_CLASS);
        log.info("Driver class loaded: {}", driverClass.getName());

        Driver driver = DriverManager.getDriver("jdbc:salesforce-datacloud:");
        log.info("Driver registered with DriverManager: {}", driver.getClass().getName());

        // Verify driver accepts our URL format
        assertThat(driver.acceptsURL("jdbc:salesforce-datacloud://test.salesforce.com"))
                .isTrue();
        log.info("Driver accepts URL format");
    }

    /**
     * Test 2: Verify connection and query execution (tests gRPC NameResolver and end-to-end functionality)
     * This is the critical test that would have caught the service file regression
     *
     * Note: This test requires credentials to be provided via system properties:
     * - test.connection.url (optional, defaults to test2.pc-rnd.salesforce.com)
     * - test.connection.userName
     * - test.connection.password
     * - test.connection.clientId
     * - test.connection.clientSecret
     */
    @Test
    @Order(2)
    void testConnectionAndQueryExecution() throws Exception {
        log.info("Test 2: Connection and Query Execution");

        // Get connection details from system properties (for CI/CD secrets)
        String jdbcUrl = System.getProperty("test.connection.url");
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = "jdbc:salesforce-datacloud://login.test2.pc-rnd.salesforce.com";
        }
        String userName = System.getProperty("test.connection.userName", "");
        String password = System.getProperty("test.connection.password", "");
        String clientId = System.getProperty("test.connection.clientId", "");
        String clientSecret = System.getProperty("test.connection.clientSecret", "");

        Properties props = new Properties();
        props.put("dataspace", "default");
        if (!userName.isEmpty()) props.setProperty("userName", userName);
        if (!password.isEmpty()) props.setProperty("password", password);
        if (!clientId.isEmpty()) props.setProperty("clientId", clientId);
        if (!clientSecret.isEmpty()) props.setProperty("clientSecret", clientSecret);

        log.info("  Attempting connection to: {}", jdbcUrl);

        // Test connection creation and query execution - this will fail if gRPC service files are broken
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            log.info("  Connection established successfully");

            // Verify connection is not closed
            assertThat(conn.isClosed()).isFalse();

            // Test query execution
            try (Statement stmt = conn.createStatement()) {
                // Try a simple query first
                try (ResultSet rs = stmt.executeQuery("SELECT 1 as test_column")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("test_column")).isEqualTo(1);
                    log.info("  Simple query executed successfully: {}", rs.getInt("test_column"));
                }
            }
        } catch (SQLException e) {
            log.error("  Connection failed: {}", e.getMessage());
            throw new AssertionError("Connection failed: " + e.getMessage(), e);
        }
    }
}
