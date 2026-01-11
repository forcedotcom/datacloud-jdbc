/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.util.HyperLogScope;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
class QueryTimeoutTest {

    @Test
    void testQueryTimeoutPropagation() throws SQLException {
        // Test that query timeout is propagated to the server-side
        try (Connection connection = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(10);
                ResultSet resultSet = statement.executeQuery("SHOW query_timeout");
                resultSet.next();
                assertEquals(resultSet.getString(1), "10s");
            }
        }
    }

    @Test
    void testLocalEnforcementDoesntImpactServerTimeout() throws SQLException {
        // Test that local enforcement delay does not affect the server-side timeout
        Properties props = new Properties();
        props.setProperty("queryTimeoutLocalEnforcementDelay", "5");
        try (Connection connection = LocalHyperTestBase.getHyperQueryConnection(props)) {
            connection.setNetworkTimeout(null, 100000);
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(10);
                ResultSet resultSet = statement.executeQuery("SHOW query_timeout");
                resultSet.next();
                assertEquals(resultSet.getString(1), "10s");
            }
        }
    }

    @Test
    void testNetworkTimeoutDoesntImpactServerTimeout() throws SQLException {
        // Test that network timeout does not affect the server-side timeout
        try (Connection connection = LocalHyperTestBase.getHyperQueryConnection()) {
            connection.setNetworkTimeout(null, 100000);
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(10);
                ResultSet resultSet = statement.executeQuery("SHOW query_timeout");
                resultSet.next();
                assertEquals(resultSet.getString(1), "10s");
            }
        }
    }

    @Test
    void testServerQueryTimeoutIsHandledCorrectly() throws SQLException {
        // Verify that in normal operations the server-side is canceling the query with the timeout (and not the client)
        try (Connection connection = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(1);
                DataCloudJDBCException ex = assertThrows(DataCloudJDBCException.class, () -> {
                    ResultSet rs = statement.executeQuery("SELECT pg_sleep(100)");
                    while (rs.next()) {
                        // Consume result
                    }
                });
                // Check that the exception contains the error message that Hyper server produces on server-side
                // timeouts
                assertThat(ex.getMessage()).contains("canceled by query timeout");
                assertEquals("57014", ex.getSQLState());
            }
        }
    }

    @Test
    @Disabled // This test is too slow, executing for 2 minutes
    void testNetworkTimeoutDoesntInterfereWithLocalEnforcement() throws SQLException {
        // Both local enforcement and network timeout integrate with the gRPC deadline mechanism.
        // This test verifies that they don't interfere with each other. If the local enforcement
        // deadline is set to 10 seconds and the network timeout is set to 100 seconds, the gRPC
        // deadline should not be 100 seconds but rather the 10 seconds.
        try (HyperLogScope logScope = new HyperLogScope()) {
            Properties props = logScope.getProperties();
            props.setProperty("queryTimeoutLocalEnforcementDelay", "10");
            try (Connection connection = LocalHyperTestBase.getHyperQueryConnection(props)) {
                connection.setNetworkTimeout(null, 100000);
                try (Statement statement = connection.createStatement()) {
                    statement.setQueryTimeout(10);
                    statement.executeQuery("SHOW query_timeout");
                }
            }

            // Verify that the gRPC deadline is far below the 100 seconds that the network timeout is set to.
            ResultSet resultSet = logScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION)from hyper_log WHERE k='grpc-query-received'");
            resultSet.next();
            assertThat(resultSet.getDouble(1)).isLessThan(20);
        }
    }
}
