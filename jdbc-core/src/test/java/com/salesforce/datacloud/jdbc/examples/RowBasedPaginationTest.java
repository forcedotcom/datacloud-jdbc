/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.hyper.HyperServerProcess;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.ManagedChannelBuilder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class RowBasedPaginationTest {
    /**
     * This example shows how to use the row based pagination mode to get results segmented by approximate row count.
     * For the example we access the results in 2 row ranges and have an implementation where the application doesn't
     * know how many results would be produced in the end
     */
    @Test
    public void testRowBasedPagination() throws SQLException {
        // Setup: Create a query that returns 10 rows
        final int totalRows = 12;
        final String sql = String.format("select s from generate_series(1, %d) s order by s asc", totalRows);
        final int pageSize = 3;
        final Duration timeout = Duration.ofSeconds(30);

        // Create a connection to the database
        final Properties properties = new Properties();
        ManagedChannelBuilder<?> channelBuilder =
                ManagedChannelBuilder.forAddress("127.0.0.1", process.getPort()).usePlaintext();

        // Step 1: Execute the query and retrieve the first page of results
        final List<Long> allResults = new ArrayList<>();
        final String queryId;
        long currentOffset = 0;

        try (final DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties);
                final DataCloudStatement stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            // Set the initial page size
            stmt.setResultSetConstraints(pageSize);
            final StreamingResultSet rs = stmt.executeQuery(sql).unwrap(StreamingResultSet.class);

            // Save the queryId for retrieving subsequent pages
            queryId = stmt.getQueryId();

            // Process the first page
            while (rs.next()) {
                allResults.add(rs.getLong(1));
            }

            // Update offset for next page
            currentOffset += rs.getRow();
        }

        // Verify we got the first page
        assertThat(allResults).containsExactly(1L, 2L, 3L);

        // Step 2: Retrieve remaining pages
        try (final DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties)) {
            final long range = currentOffset + pageSize;
            QueryStatus status = conn.waitFor(queryId, timeout, t -> t.getRowCount() >= range);

            while (true) {
                final boolean shouldCheck = !status.allResultsProduced() && currentOffset >= status.getRowCount();
                if (shouldCheck) {
                    status = conn.waitFor(queryId, timeout, t -> t.getRowCount() >= range);
                }

                final boolean readAllRows = status.allResultsProduced() && currentOffset >= status.getRowCount();
                if (readAllRows) {
                    break;
                }

                final DataCloudResultSet rs = conn.getRowBasedResultSet(queryId, currentOffset, pageSize);

                final List<Long> pageResults = new ArrayList<>();
                while (rs.next()) {
                    pageResults.add(rs.getLong(1));
                }
                allResults.addAll(pageResults);

                // Update offset for next page
                currentOffset += rs.getRow();

                log.warn("Retrieved page. offset={}, values={}", currentOffset - rs.getRow(), pageResults);
            }
        }

        // Verify we got all expected results in order
        List<Long> expected = LongStream.rangeClosed(1, totalRows).boxed().collect(Collectors.toList());
        assertThat(allResults).containsExactlyElementsOf(expected);
    }

    static HyperServerProcess process;

    @BeforeAll
    static void beforeAll() {
        // Here we use default.yaml which doesn't override result_target_chunk_size and
        // arrow_write_buffer_initial_tuple_limit -- using those settings caused the first "page" too small to be
        // meaningful in a test (1 row)
        process = new HyperServerProcess("default.yaml");
    }

    @SneakyThrows
    @AfterAll
    static void afterAll() {
        process.close();
    }
}
