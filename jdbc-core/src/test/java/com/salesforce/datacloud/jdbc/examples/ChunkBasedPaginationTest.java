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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.ManagedChannelBuilder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class ChunkBasedPaginationTest {
    /**
     * This example shows how to use the chunk based pagination mode to process large result sets.
     * The query executes asynchronously, and we retrieve results one chunk at a time until all chunks
     * have been processed.
     */
    @Test
    public void testChunkBasedPagination() throws SQLException {
        val sql =
                "select a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c, cast(a as numeric(38,18)) d from generate_series(1, 1024 * 1024 * 10) as s(a) order by a asc";
        val timeout = Duration.ofSeconds(30);
        val offset = new AtomicLong(0);
        val properties = new Properties();

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                        "127.0.0.1", HyperTestBase.getInstancePort())
                .usePlaintext();

        final String queryId;

        try (final DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties);
                final DataCloudStatement stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            queryId = stmt.executeAsyncQuery(sql).getQueryId();
        }

        int prev = 1;
        DataCloudQueryStatus status = null;
        while (true) {
            try (final DataCloudConnection conn = DataCloudConnection.of(channelBuilder, properties)) {
                if (status == null || !status.allResultsProduced()) {
                    status = conn.waitForChunksAvailable(
                            queryId, offset.get(), 1, timeout, false); // false because we're waiting for the next chunk
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
    }
}
