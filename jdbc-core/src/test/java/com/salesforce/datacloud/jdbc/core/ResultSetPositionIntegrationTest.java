/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Checks {@code isBeforeFirst}, {@code isAfterLast} and {@code isFirst} against a real Hyper server
 * for each way a result set is produced: adaptive ({@code executeQuery}), chunk-based and row-based.
 */
@ExtendWith(LocalHyperTestBase.class)
class ResultSetPositionIntegrationTest {
    private static final String ROWS_SQL = "select s from generate_series(1, 5) s order by s asc";
    private static final String EMPTY_SQL = "select s from generate_series(1, 5) s where s > 100";

    @Test
    @SneakyThrows
    void adaptiveWithRows() {
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        try (val conn = getHyperQueryConnection(server);
                val stmt = conn.createStatement();
                val rs = stmt.executeQuery(ROWS_SQL)) {
            assertThat(readAllWithPositionChecks(rs)).containsExactly(1L, 2L, 3L, 4L, 5L);
        }
    }

    @Test
    @SneakyThrows
    void adaptiveEmpty() {
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        try (val conn = getHyperQueryConnection(server);
                val stmt = conn.createStatement();
                val rs = stmt.executeQuery(EMPTY_SQL)) {
            assertEmptyPositions(rs);
        }
    }

    @Test
    @SneakyThrows
    void chunkBasedWithRows() {
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        try (val conn = getHyperQueryConnection(server);
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            val queryId = stmt.executeAsyncQuery(ROWS_SQL).getQueryId();
            val status = conn.waitFor(queryId, QueryStatus::allResultsProduced);
            try (val rs = conn.getChunkBasedResultSet(queryId, 0, status.getChunkCount())) {
                assertThat(readAllWithPositionChecks(rs)).containsExactly(1L, 2L, 3L, 4L, 5L);
            }
        }
    }

    @Test
    @SneakyThrows
    void rowBasedWithRows() {
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        try (val conn = getHyperQueryConnection(server);
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            val queryId = stmt.executeAsyncQuery(ROWS_SQL).getQueryId();
            val status = conn.waitFor(queryId, QueryStatus::allResultsProduced);
            try (val rs = conn.getRowBasedResultSet(queryId, 0, status.getRowCount())) {
                assertThat(readAllWithPositionChecks(rs)).containsExactly(1L, 2L, 3L, 4L, 5L);
            }
        }
    }

    @Test
    @SneakyThrows
    void rowBasedEmpty() {
        val server = HyperServerManager.get(ConfigFile.DEFAULT);
        try (val conn = getHyperQueryConnection(server);
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            val queryId = stmt.executeAsyncQuery(EMPTY_SQL).getQueryId();
            conn.waitFor(queryId, QueryStatus::allResultsProduced);
            try (val rs = conn.getRowBasedResultSet(queryId, 0, 1)) {
                assertEmptyPositions(rs);
            }
        }
    }

    /** Drives the cursor through before-first, each row and after-last, asserting the flags at every step. */
    @SneakyThrows
    private static List<Long> readAllWithPositionChecks(ResultSet rs) {
        assertThat(rs.isBeforeFirst()).isTrue();
        assertThat(rs.isAfterLast()).isFalse();
        assertThat(rs.isFirst()).isFalse();

        val values = new ArrayList<Long>();
        while (rs.next()) {
            assertThat(rs.isBeforeFirst()).isFalse();
            assertThat(rs.isAfterLast()).isFalse();
            assertThat(rs.isFirst()).isEqualTo(values.isEmpty());
            values.add(rs.getLong(1));
        }

        assertThat(rs.isBeforeFirst()).isFalse();
        assertThat(rs.isFirst()).isFalse();
        assertThat(rs.isAfterLast()).isTrue();
        return values;
    }

    @SneakyThrows
    private static void assertEmptyPositions(ResultSet rs) {
        assertThat(rs.isBeforeFirst())
                .as("an empty result has no row to be before")
                .isFalse();
        assertThat(rs.isAfterLast()).isFalse();
        assertThat(rs.next()).isFalse();
        assertThat(rs.isBeforeFirst()).isFalse();
        assertThat(rs.isAfterLast())
                .as("an empty result has no last row to be after")
                .isFalse();
        assertThat(rs.isFirst()).isFalse();
    }
}
