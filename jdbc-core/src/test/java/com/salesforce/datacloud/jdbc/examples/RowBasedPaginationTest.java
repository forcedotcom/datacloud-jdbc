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
package com.salesforce.datacloud.jdbc.examples;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.core.partial.RowBased;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.ManagedChannelBuilder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class RowBasedPaginationTest {
    /**
     * This example shows how to use the row based pagination mode to get results segmented by approximate row count.
     * For the example we access the results in 2 row ranges and have an implementation where the application doesn't
     * know how many results would be produced in the end
     */
    @Test
    public void testRowBasedPagination() throws SQLException {
        // Setup: Create a query that returns 10 rows
        final int totalRows = 10;
        final String sql = String.format("select s from generate_series(1, %d) s order by s asc", totalRows);
        final int pageSize = 2;
        final Duration timeout = Duration.ofSeconds(30);

        // Create a connection to the database
        final Properties properties = new Properties();
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

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
        assertThat(allResults).containsExactly(1L, 2L);

        // Step 2: Retrieve remaining pages
        try (final DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties)) {
            DataCloudQueryStatus status = conn.waitForRowsAvailable(queryId, currentOffset, pageSize, timeout, true);

            while (true) {
                final boolean shouldCheck = !status.allResultsProduced() && currentOffset >= status.getRowCount();
                if (shouldCheck) {
                    status = conn.waitForRowsAvailable(queryId, currentOffset, pageSize, timeout, true);
                }

                final boolean readAllRows = status.allResultsProduced() && currentOffset >= status.getRowCount();
                if (readAllRows) {
                    break;
                }

                final DataCloudResultSet rs =
                        conn.getRowBasedResultSet(queryId, currentOffset, pageSize, RowBased.Mode.SINGLE_RPC);

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
}
