/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.accessor.impl.DataCloudArray;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.util.HyperLogScope;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
public class DataCloudConnectionFunctionalTest {
    @Test
    public void testNetworkTimeoutDefault() throws SQLException {
        // Verify that by default no deadline is set
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = LocalHyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {
            assertThat(connection.getNetworkTimeout()).isZero();
            // Make a call to capture the gRPC deadline
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            // If the caller has no deadline set the server side will report a super high deadline
            val rs = hyperLogScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION)from hyper_log WHERE k='grpc-query-received'");
            rs.next();
            assertThat(rs.getDouble(1)).isGreaterThan(Duration.ofDays(5).toMillis() / 1000.0);
        }
        hyperLogScope.close();
    }

    @Test
    public void testNetworkTimeoutPropagatesToServer() throws SQLException {
        // Verify that when network timeout is set a corresponding deadline is set on the gRPC call level
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = LocalHyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {
            // Set the network timeout to 5 seconds
            connection.setNetworkTimeout(null, 5000);
            assertThat(connection.getNetworkTimeout()).isEqualTo(5000);
            // Make a call to capture the gRPC deadline
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            // Verify the deadline propagated to HyperServerConfig, and that it's less than the default (which is test
            // in
            // other test case)
            val rs = hyperLogScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION)from hyper_log WHERE k='grpc-query-received'");
            rs.next();
            assertThat(rs.getDouble(1)).isLessThan(5);
        }
        hyperLogScope.close();
    }

    @Test
    @SneakyThrows
    public void testNetworkTimeoutIsPerGrpcCall() {
        // This is a regression test as we previously had set the deadline on the stub which results in a deadline
        // across all calls made on that stub. While the desired network timeout behavior is that it should
        // independently apply on each call. Thus in this test we first create a stub and then check that after a sleep
        // there still is approximately the full network timeout duration for the next call.
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = LocalHyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {
            // Set the network timeout to 5 seconds
            connection.setNetworkTimeout(null, 5000);
            assertThat(connection.getNetworkTimeout()).isEqualTo(5000);

            // Do an initial call to ensure that the stub deadline would get definitely started (and that we don't
            // accidentally rely on some internal behavior)
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }
            // Sleep for 1.1 second after which a shared deadline would not be able to be >4s anymore (at most 3.9s)
            Thread.sleep(1100);
            // Do a second call and we'll verify that the deadline is still approximately 5 seconds from now
            try (val statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            // Verify that all the deadlines are still >4s
            // We allow up to 1 second of slack to account for busy local testing machine
            val rs = hyperLogScope.executeQuery(
                    "select CAST(v->>'requested-timeout' as DOUBLE PRECISION) from hyper_log WHERE k='grpc-query-received'");
            rs.next();
            assertThat(rs.getDouble(1)).isLessThan(5);
            assertThat(rs.getDouble(1)).isGreaterThan(4);
            rs.next();
            assertThat(rs.getDouble(1)).isLessThan(5);
            assertThat(rs.getDouble(1)).isGreaterThan(4);
            assertThat(rs.next()).isFalse();
        }
        hyperLogScope.close();
    }

    @Test
    public void testUseAfterResultSetClosed() throws SQLException {
        // Setup the SQL query so that it has a large ARRAY first and then only small ARRAYs. We'll keep the reference
        // on the first one and validate that it remains valid during the whole execution. The "filler" arrays are setup
        // in a way so that several chunks are created to ensure test coverage on chunk updates.
        String query =
                "SELECT '[abc,def,ghi]'::text[] as a, 0 as g UNION ALL SELECT ARRAY[rpad('', 1024*1024, 'x')] as a, g FROM generate_series(1, 200) g ORDER BY g ASC";

        try (DataCloudConnection connection = HyperTestBase.getHyperQueryConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(query);
                // Access the first row
                rs.next();
                Array reference = rs.getArray(1);

                while (rs.next()) {
                    // We don't modify the reference and thus expect that the data remains unchanged and accessible
                    // during the runtime of the query
                    assertThat(((Object[]) reference.getArray()).length).isEqualTo(3);
                    Object entry = ((Object[]) reference.getArray())[2];
                    assertThat(entry).isEqualTo(new Text("ghi"));
                }
            }
        }
    }

    /**
     * This is a regression test for a Bug where the Array object returned by the ResultSet was invalidated
     * after the ResultSet was closed / after the ResultSet was moving on between the backing Chunks.
     */
    @Test
    @SneakyThrows
    public void testDataCloudArrayPersistenceAfterResultSetClose() {
        // Integration test demonstrating that DataCloudArray data persists after ResultSet is closed
        // This test would have failed before the fix due to data loss
        HyperLogScope hyperLogScope = new HyperLogScope();
        try (val connection = HyperTestBase.getHyperQueryConnection(hyperLogScope.getProperties())) {

            try (val statement = connection.createStatement()) {
                val query = "SELECT '[abc,null,def,ghi,jkl,mno]'::text[] as strings_";

                List<List<Object>> rows = new ArrayList<>();

                // Process ResultSet and close it
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        rows.add(getNextRow(resultSet, columnCount));
                    }
                } // ResultSet is now closed

                // Access array data after ResultSet is closed - this should work now
                assertThat(rows).hasSize(1);
                List<Object> row = rows.get(0);
                assertThat(row).hasSize(1);

                Object obj = row.get(0);
                assertThat(obj).isInstanceOf(DataCloudArray.class);

                DataCloudArray array = (DataCloudArray) obj;
                Object[] arrayData = (Object[]) array.getArray();

                // Verify array data is accessible and correct
                assertThat(arrayData).hasSize(6);
                assertThat(arrayData[0].toString()).isEqualTo("abc");
                assertThat(arrayData[1]).isNull(); // NULL value
                assertThat(arrayData[2].toString()).isEqualTo("def");
                assertThat(arrayData[3].toString()).isEqualTo("ghi");
                assertThat(arrayData[4].toString()).isEqualTo("jkl");
                assertThat(arrayData[5].toString()).isEqualTo("mno");

                // Test JDBC 1-based indexing
                Object[] subArray = (Object[]) array.getArray(3, 3); // Get elements 3, 4, 5 (def, ghi, jkl)
                assertThat(subArray).hasSize(3);
                assertThat(subArray[0].toString()).isEqualTo("def");
                assertThat(subArray[1].toString()).isEqualTo("ghi");
                assertThat(subArray[2].toString()).isEqualTo("jkl");

                // Test JDBC 1-based indexing with NULL value
                Object[] subArrayWithNull = (Object[]) array.getArray(2, 3); // Get elements 2, 3, 4 (null, def, ghi)
                assertThat(subArrayWithNull).hasSize(3);
                assertThat(subArrayWithNull[0]).isNull(); // NULL value
                assertThat(subArrayWithNull[1].toString()).isEqualTo("def");
                assertThat(subArrayWithNull[2].toString()).isEqualTo("ghi");
            }
        }
        hyperLogScope.close();
    }

    private List<Object> getNextRow(ResultSet resultSet, int columnCount) throws SQLException {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            row.add(resultSet.getObject(i));
        }
        return row;
    }
}
