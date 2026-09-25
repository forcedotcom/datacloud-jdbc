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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
     * The authentication flows exercised by {@link #testConnectionAndQueryExecution(AuthFlow)}, in
     * the order they run. CLIENT_CREDENTIALS is the recommended flow and runs first. The deprecated
     * username/password flow is intentionally not included here.
     */
    private enum AuthFlow {
        CLIENT_CREDENTIALS,
        PRIVATE_KEY,
        REFRESH_TOKEN
    }

    /**
     * Test 2: Verify connection and query execution (tests gRPC NameResolver and end-to-end functionality)
     * across every supported (non-deprecated) authentication flow. This is the critical test that
     * would have caught the service file regression.
     *
     * Each flow only runs if its required system properties are supplied; otherwise it is skipped
     * (not failed) via JUnit assumptions:
     * - CLIENT_CREDENTIALS: test.connection.orgDomainUrl, clientId, clientSecret
     * - PRIVATE_KEY: test.connection.userName, privateKey, clientId
     * - REFRESH_TOKEN: test.connection.refreshToken, clientId, clientSecret
     *
     * test.connection.url (optional, defaults to test2.pc-rnd.salesforce.com) is used by every flow
     * except CLIENT_CREDENTIALS, which requires its org's My Domain URL rather than the login/pod URL.
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @Order(2)
    @EnumSource(AuthFlow.class)
    void testConnectionAndQueryExecution(AuthFlow flow) throws Exception {
        log.info("Test 2: Connection and Query Execution ({})", flow);

        Properties props = new Properties();
        props.put("dataspace", "default");
        String jdbcUrl = buildPropsForFlow(flow, props);

        log.info("  Attempting connection to: {} using {}", jdbcUrl, flow);

        // Test connection creation and query execution - this will fail if gRPC service files are broken
        // The test OAuth endpoint at login.test2.pc-rnd.salesforce.com intermittently returns
        // HTTP 400 {"error":"unknown_error","error_description":"retry your request"}. This is
        // treated as test-environment instability: HTTP 400 is not retriable per HTTP semantics,
        // so the driver is correct not to retry it. If the same behavior is observed outside the
        // test environment, the upstream service should return an appropriate (5xx / 429) status
        // and the fix belongs in the driver's core retry policy, not here.
        try (Connection conn = connectWithRetry(jdbcUrl, props)) {
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

    /**
     * Populates {@code props} with the credentials required for {@code flow} and returns the JDBC
     * URL to connect with. Skips the test (via {@link Assumptions}) if the required system
     * properties for that flow are not present.
     */
    private static String buildPropsForFlow(AuthFlow flow, Properties props) {
        String clientId = System.getProperty("test.connection.clientId", "");

        switch (flow) {
            case CLIENT_CREDENTIALS: {
                String url = System.getProperty("test.connection.orgDomainUrl", "");
                String clientSecret = System.getProperty("test.connection.clientSecret", "");
                Assumptions.assumeTrue(
                        isNotEmpty(url) && isNotEmpty(clientId) && isNotEmpty(clientSecret),
                        "Skipping CLIENT_CREDENTIALS: requires test.connection.orgDomainUrl, clientId, clientSecret");
                props.setProperty("clientId", clientId);
                props.setProperty("clientSecret", clientSecret);
                return toJdbcUrl(url);
            }
            case PRIVATE_KEY: {
                String userName = System.getProperty("test.connection.userName", "");
                String privateKey = System.getProperty("test.connection.privateKey", "");
                Assumptions.assumeTrue(
                        isNotEmpty(userName) && isNotEmpty(privateKey) && isNotEmpty(clientId),
                        "Skipping PRIVATE_KEY: requires test.connection.userName, privateKey, clientId");
                props.setProperty("userName", userName);
                props.setProperty("privateKey", privateKey);
                props.setProperty("clientId", clientId);
                return defaultJdbcUrl();
            }
            case REFRESH_TOKEN: {
                String refreshToken = System.getProperty("test.connection.refreshToken", "");
                String clientSecret = System.getProperty("test.connection.clientSecret", "");
                Assumptions.assumeTrue(
                        isNotEmpty(refreshToken) && isNotEmpty(clientId) && isNotEmpty(clientSecret),
                        "Skipping REFRESH_TOKEN: requires test.connection.refreshToken, clientId, clientSecret");
                props.setProperty("refreshToken", refreshToken);
                props.setProperty("clientId", clientId);
                props.setProperty("clientSecret", clientSecret);
                return defaultJdbcUrl();
            }
            default:
                throw new IllegalStateException("Unhandled auth flow: " + flow);
        }
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    private static String defaultJdbcUrl() {
        String url = System.getProperty("test.connection.url", "");
        return isNotEmpty(url) ? url : "jdbc:salesforce-datacloud://login.test2.pc-rnd.salesforce.com";
    }

    /**
     * Accepts either a bare host, an https:// URL (as found in an org's My Domain field), or an
     * already-formed jdbc:salesforce-datacloud:// URL, and normalizes it to the latter.
     */
    private static String toJdbcUrl(String hostOrUrl) {
        if (hostOrUrl.startsWith("jdbc:")) {
            return hostOrUrl;
        }
        return "jdbc:salesforce-datacloud://" + hostOrUrl.replaceFirst("^https?://", "");
    }

    /**
     * Opens a JDBC connection with a bounded retry on a test-environment-specific OAuth
     * response ({@code HTTP 400 {"error":"unknown_error","error_description":"retry your request"}})
     * from {@code login.test2.pc-rnd.salesforce.com}. This is scoped to the integration test
     * because HTTP 400 is non-retriable per standard HTTP semantics — the driver is correct
     * not to retry it. If this pattern ever appears against a non-test environment, the upstream
     * service should be changed to return an appropriate retriable status (5xx / 429) and the
     * driver's core retry policy will then handle it automatically.
     *
     * Exponential backoff capped at a total of ~60 seconds of retries.
     */
    private static Connection connectWithRetry(String jdbcUrl, Properties props) throws SQLException {
        final long deadline = System.currentTimeMillis() + 60_000L;
        long delayMs = 1_000L;
        int attempt = 0;
        SQLException lastTransient = null;
        while (true) {
            attempt++;
            try {
                return DriverManager.getConnection(jdbcUrl, props);
            } catch (SQLException e) {
                if (!isTransientOAuthRetryRequest(e)) {
                    throw e;
                }
                lastTransient = e;
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw e;
                }
                long sleep = Math.min(delayMs, remaining);
                log.warn(
                        "  OAuth returned transient 'retry your request' (attempt {}); sleeping {}ms before retry",
                        attempt,
                        sleep);
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw lastTransient;
                }
                delayMs = Math.min(delayMs * 2, 15_000L);
            }
        }
    }

    private static boolean isTransientOAuthRetryRequest(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("HTTP 400") && msg.contains("retry your request")) {
                return true;
            }
        }
        return false;
    }
}
