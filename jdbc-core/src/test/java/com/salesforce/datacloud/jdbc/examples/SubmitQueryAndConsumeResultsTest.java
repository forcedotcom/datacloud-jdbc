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

import static java.lang.Math.min;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.partial.RowBased;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.ManagedChannelBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This example uses a locally spawned Hyper instance to demonstrate best practices around connecting to Hyper.
 * This consciously only uses the JDBC API in the core and no helpers (outside of this class) to provide self contained
 * examples.
 */
@Slf4j
@ExtendWith(HyperTestBase.class)
public class SubmitQueryAndConsumeResultsTest {
    /**
     * This example shows how to create a Data Cloud Connection while still having full control over concerns like
     * authorization and tracing.
     */
    @Test
    public void testBareBonesExecuteQuery() throws SQLException {
        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels, setup in the way you like (mTLS / Plaintext / ...) and your own
        // interceptors as well as executors.
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        // Use the JDBC Driver interface
        try (DataCloudConnection conn = DataCloudConnection.fromChannel(channel, properties)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(1,10) s");
                while (rs.next()) {
                    System.out.println("Retrieved value:" + rs.getLong(1));
                }
            }
        }
    }

    /**
     * Analyze the query status, as we have a query status we know that the query was last observed in a non failing
     * state.
     *
     * Offset must always be larger or equal to get row count (which would happen for typical next based pagination)
     */
    private static long rowBasedStatusObjectRowsCheck(DataCloudQueryStatus queryStatus, long offset, long pageLimit) {
        // Check if we can at least return some data
        if (queryStatus.getRowCount() > offset) {
            return min(queryStatus.getRowCount() - offset, pageLimit);
        }
        // A negative count signals that no data is available
        return -1;
    }

    /**
     * Checks if the query status signals that all results are produced
     */
    private static boolean allResultsProduced(DataCloudQueryStatus queryStatus) {
        return queryStatus.isResultProduced() || queryStatus.isExecutionFinished();
    }

    /**
     * This example shows how to use the row based pagination mode to get results segmented by approximate row count.
     * For the example we access the results in 2 row ranges and have an implementation where the application doesn't
     * know how many results would be produced in the end
     */
    @Test
    public void testRowBasedPagination() throws SQLException {
        final int pageRowLimit = 2;
        long offset = 0;
        long page = 0;

        // The connection properties
        Properties properties = new Properties();

        // You can bring your own gRPC channels, setup in the way you like (mTLS / Plaintext / ...) and your own
        // interceptors as well as executors.
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        try (DataCloudConnection conn = DataCloudConnection.fromChannel(channel, properties)) {
            // Submit the query and consume the initial page
            String queryId;
            try (Statement stmt = conn.createStatement()) {
                log.warn("Executing query using a single `ExecuteQuery` RPC Call");
                ResultSet rs = stmt.executeQuery("SELECT s FROM generate_series(1,11) s");
                queryId = ((DataCloudResultSet) rs).getQueryId();
                // For this result set we as a consumer must currently implement the pagination limit ourselves
                int i = 0;
                while (rs.next() && (i++ < pageRowLimit)) {
                    ++offset;
                    System.out.println("Retrieved value: " + rs.getLong(1) + " on page " + page);
                }
                ++page;
            }

            // Consume further pages until the full result is consumed (could also be done on a new connection if
            // needed)
            // NIT: We should provide an API on the original result set to access the `DataCloudQueryStatus` that way,
            // if the query is already finished we don't need to do another network round-trip.
            Optional<DataCloudQueryStatus> cachedStatus = Optional.empty();
            while (true) {
                // Try to make sure we have a status object
                if (!cachedStatus.isPresent()) {
                    // Identify if there is more data?
                    long lambdaOffset = offset;
                    // In case of query error this could throw an runtime exception
                    // NIT: What is the timeout enforced here?
                    log.warn("Fetching query status using a single `GetQueryInfo` RPC call");
                    // NIT: Semantically I would want takeWhile here which is only available in Java 11
                    cachedStatus = conn.getQueryStatus(queryId)
                            .filter(queryStatus ->
                                    (rowBasedStatusObjectRowsCheck(queryStatus, lambdaOffset, pageRowLimit) > 0)
                                            || allResultsProduced(queryStatus))
                            .findFirst();

                    // Query is still running
                    // NIT: Check how we should handle this in the presence of timeouts
                    if (!cachedStatus.isPresent()) {
                        continue;
                    }
                }

                long availableRows = rowBasedStatusObjectRowsCheck(cachedStatus.get(), offset, pageRowLimit);
                // Check if query completed and thus we can't produce more results
                if (availableRows <= 0) {
                    if (allResultsProduced(cachedStatus.get())) {
                        break;
                    } else {
                        // We need to fetch a new status in the next iteration
                        // Due to the long-polling nature of `conn.getQueryStatus` this doesn't result in a busy
                        // spinning loop even if the query is still executing
                        cachedStatus = Optional.empty();
                        continue;
                    }
                }

                // At this point we know that rows are available
                log.warn("Fetching query status using a single `GetQueryResult` RPC call");
                try (ResultSet rs =
                        conn.getRowBasedResultSet(queryId, offset, pageRowLimit, RowBased.Mode.SINGLE_RPC)) {
                    while (rs.next()) {
                        ++offset;
                        System.out.println("Retrieved value: " + rs.getLong(1) + " on page " + page);
                    }
                    ++page;
                }
            }
        }
        log.warn("Completed");
    }
}
