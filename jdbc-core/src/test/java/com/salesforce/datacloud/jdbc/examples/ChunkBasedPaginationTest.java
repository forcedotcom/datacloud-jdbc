/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class ChunkBasedPaginationTest {
    /**
     * This example shows how to use the chunk based pagination mode to process large result sets.
     * The query executes asynchronously, and we retrieve results one chunk at a time until all chunks
     * have been processed.
     */
    @Test
    public void testChunkBasedPagination() throws SQLException {
        val sql =
                "select a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c, cast(a as numeric(38,18)) d from generate_series(1, 64) as s(a) order by a asc";
        val timeout = Duration.ofSeconds(30);
        val offset = new AtomicLong(0);

        final String queryId;

        try (final DataCloudConnection conn = getHyperQueryConnection();
                final DataCloudStatement stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            queryId = stmt.executeAsyncQuery(sql).getQueryId();
        }

        int prev = 1;
        QueryStatus status = null;
        while (true) {
            try (final DataCloudConnection conn = getHyperQueryConnection()) {
                if (status == null || !status.allResultsProduced()) {
                    status = conn.waitFor(queryId, t -> t.getChunkCount() > offset.get());
                }

                if (status.allResultsProduced() && offset.get() >= status.getChunkCount()) {
                    log.warn("All chunks have been consumed");
                    break;
                }

                final long chunk = offset.getAndAdd(1);
                final DataCloudResultSet rs = conn.getChunkBasedResultSet(queryId, chunk);

                while (rs.next()) {
                    assertThat(rs.getLong(1)).isEqualTo(prev++);
                }
            }
        }

        assertThat(status.getChunkCount())
                .as("we should have seen more than one chunk, last=" + status)
                .isGreaterThan(1);
    }
}
