/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This example shows how to retrieve the wrapper {@link QueryStatus} from a
 * {@link DataCloudStatement} for both supported execution paths:
 * <ul>
 *   <li>The adaptive (synchronous) path via {@link DataCloudStatement#executeQuery(String)}.</li>
 *   <li>The async path via {@link DataCloudStatement#executeAsyncQuery(String)}.</li>
 * </ul>
 *
 * <p>{@link DataCloudStatement#getQueryStatus()} is a non-blocking accessor that reflects the
 * latest status the driver has observed. It issues no additional RPCs. Once the caller has
 * iterated the {@link ResultSet} to completion, the returned status is terminal and -- when the
 * server supplied it -- carries {@link QueryStatus#getExecutionStatistics() execution statistics}.
 */
@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class QueryStatusTest {

    private static final String SQL = "SELECT s FROM generate_series(1, 100) s";

    /**
     * Adaptive path: {@link DataCloudStatement#executeQuery(String)} returns a streaming
     * {@link ResultSet}. The query completes as the caller drains the result set, and the
     * final {@link QueryStatus} -- including execution statistics -- is available via
     * {@link DataCloudStatement#getQueryStatus()}.
     */
    @Test
    public void testGetQueryStatusAdaptivePath() throws SQLException {
        try (val conn = getHyperQueryConnection();
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            val rs = stmt.executeQuery(SQL);
            while (rs.next()) {
                // drain the result set; this drives the query to completion
            }

            val status = stmt.getQueryStatus();
            log.info(
                    "Adaptive path status: queryId={}, completion={}",
                    status.getQueryId(),
                    status.getCompletionStatus());

            assertThat(status.getQueryId()).isNotEmpty();
            assertThat(status.allResultsProduced()).isTrue();
            assertThat(status.getExecutionStatistics()).isNotNull();
            assertThat(status.getExecutionStatistics().getWallClockTime()).isNotNull();
        }
    }

    /**
     * Async path: {@link DataCloudStatement#executeAsyncQuery(String)} submits the query and
     * returns immediately with a query id. The initial status is available right away, but it
     * does not yet include execution statistics. Once the caller retrieves results via
     * {@link DataCloudStatement#getResultSet()} -- which internally waits for completion --
     * the final status (with stats) is available from {@link DataCloudStatement#getQueryStatus()}.
     */
    @Test
    public void testGetQueryStatusAsyncPath() throws SQLException {
        try (val conn = getHyperQueryConnection();
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            stmt.executeAsyncQuery(SQL);

            // Immediately after submission the initial status has a query id but no stats yet.
            val initial = stmt.getQueryStatus();
            log.info("Async path initial status: queryId={}", initial.getQueryId());
            assertThat(initial.getQueryId()).isNotEmpty();
            assertThat(initial.getExecutionStatistics()).isNull();

            // Fetch and drain the result set; this waits for query completion.
            val rs = stmt.getResultSet();
            while (rs.next()) {
                // drain
            }

            val terminal = stmt.getQueryStatus();
            log.info(
                    "Async path terminal status: queryId={}, completion={}",
                    terminal.getQueryId(),
                    terminal.getCompletionStatus());

            assertThat(terminal.allResultsProduced()).isTrue();
            assertThat(terminal.getExecutionStatistics()).isNotNull();
            assertThat(terminal.getExecutionStatistics().getWallClockTime()).isNotNull();
        }
    }
}
