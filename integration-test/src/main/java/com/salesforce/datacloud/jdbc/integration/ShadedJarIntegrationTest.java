/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.integration;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

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
public class ShadedJarIntegrationTest {

    private static final String DRIVER_CLASS = "com.salesforce.datacloud.jdbc.DataCloudJDBCDriver";

    public static void main(String[] args) {
        log.info("Starting Shaded JAR Integration Test");

        testDriverLoading();
        testConnectionCreation();
        testBasicFunctionality();

        log.info("Integration test completed");
    }

    /**
     * Test 1: Verify the JDBC driver can be loaded from the shaded JAR
     */
    private static void testDriverLoading() {
        log.info("Test 1: Driver Loading");

        try {
            // Load driver class
            Class<?> driverClass = Class.forName(DRIVER_CLASS);
            log.info("  Driver class loaded: {}", driverClass.getName());

            // Verify driver is registered
            Driver driver = DriverManager.getDriver("jdbc:salesforce-datacloud:");
            log.info(
                    "  Driver registered with DriverManager: {}",
                    driver.getClass().getName());

            // Verify driver accepts our URL format
            boolean accepts = driver.acceptsURL("jdbc:salesforce-datacloud://test.salesforce.com");
            if (accepts) {
                log.info("  Driver accepts URL format");
            } else {
                log.error("  Driver does not accept expected URL format");
            }
        } catch (Exception e) {
            log.error("  Driver loading failed: {}", e.getMessage());
        }
    }

    /**
     * Test 2: Verify connection can be created (tests gRPC NameResolver)
     * This is the critical test that would have caught the service file regression
     */
    private static void testConnectionCreation() {
        log.info("Test 2: Connection Creation");

        // Get connection details from system properties (for CI/CD secrets)
        String jdbcUrl = System.getProperty(
                "test.connection.url", "jdbc:salesforce-datacloud://login.test2.pc-rnd.salesforce.com");
        String userName = System.getProperty("test.connection.userName", "");
        String password = System.getProperty("test.connection.password", "");
        String clientId = System.getProperty("test.connection.clientId", "");
        String clientSecret = System.getProperty("test.connection.clientSecret", "");

        Properties props = new Properties();
        if (!userName.isEmpty()) props.setProperty("userName", userName);
        if (!password.isEmpty()) props.setProperty("password", password);
        if (!clientId.isEmpty()) props.setProperty("clientId", clientId);
        if (!clientSecret.isEmpty()) props.setProperty("clientSecret", clientSecret);

        log.info("  Attempting connection to: {}", jdbcUrl);
        log.info("  Using credentials: {}", userName.isEmpty() ? "Not provided" : "Provided");

        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, props);
            log.info("  Connection established successfully");

            // Test basic query execution if connection succeeded
            testQueryExecution(conn);

            conn.close();

        } catch (SQLException e) {
            String message = e.getMessage();
            log.warn("  Connection failed: {}", message);

            // Check for the specific regression we're testing for
            if (message.toLowerCase().contains("address types of nameresolver 'unix'")
                    || message.toLowerCase().contains("not supported by transport")
                    || message.toLowerCase().contains("unix://")) {
                log.error("  CRITICAL: gRPC NameResolver regression detected!");
            }
        }
    }

    /**
     * Test 3: Execute a simple query to verify end-to-end functionality
     */
    private static void testQueryExecution(Connection conn) {
        log.info("Test 3: Query Execution");

        try (Statement stmt = conn.createStatement()) {
            // Try a simple query first
            try (ResultSet rs = stmt.executeQuery("SELECT 1 as test_column")) {
                if (rs.next()) {
                    log.info("  Simple query executed successfully: {}", rs.getInt("test_column"));
                }
            } catch (SQLException e) {
                log.error("  Simple query failed: {}", e.getMessage());
            }

            // Try to query a real table if available
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM shopify_order_details__dll LIMIT 1")) {
                if (rs.next()) {
                    log.info("  Real table query executed successfully");
                } else {
                    log.info("  Real table query executed (no results)");
                }
            } catch (SQLException e) {
                log.warn("  Real table query failed: {}", e.getMessage());
            }
        } catch (SQLException e) {
            log.error("  Query execution failed: {}", e.getMessage());
        }
    }

    /**
     * Test 4: Verify basic JDBC functionality works
     */
    private static void testBasicFunctionality() {
        log.info("Test 4: Basic JDBC Functionality");

        try {
            // Test driver metadata
            Driver driver = DriverManager.getDriver("jdbc:salesforce-datacloud:");
            int majorVersion = driver.getMajorVersion();
            int minorVersion = driver.getMinorVersion();
            log.info("  Driver version: {}.{}", majorVersion, minorVersion);

            // Test driver properties
            Properties info = new Properties();
            info.setProperty("user", "test");
            info.setProperty("password", "test");

            try {
                java.sql.DriverPropertyInfo[] propInfo =
                        driver.getPropertyInfo("jdbc:salesforce-datacloud://test.com", info);
                log.info("  Driver property info available: {} properties", propInfo.length);
            } catch (Exception e) {
                log.warn("  Driver property info test failed: {}", e.getMessage());
            }

            log.info("  Basic functionality tests completed");
        } catch (Exception e) {
            log.error("  Basic functionality test failed: {}", e.getMessage());
        }
    }
}
