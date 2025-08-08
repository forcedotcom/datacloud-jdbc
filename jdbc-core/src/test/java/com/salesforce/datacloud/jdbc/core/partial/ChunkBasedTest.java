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
package com.salesforce.datacloud.jdbc.core.partial;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
class ChunkBasedTest {
    @SneakyThrows
    private List<Integer> sut(String queryId, long chunkId, long limit) {
        try (val connection = getHyperQueryConnection()) {
            val rs = limit == 1
                    ? connection.getChunkBasedResultSet(queryId, chunkId)
                    : connection.getChunkBasedResultSet(queryId, chunkId, limit);
            return RowBasedTest.toStream(rs).collect(Collectors.toList());
        }
    }

    private static final int smallSize = 4;
    private static final int largeSize = smallSize * 16;
    private static String singleChunk;
    private static String multipleChunks;

    @SneakyThrows
    @BeforeAll
    static void setupQueries() {
        singleChunk = getQueryId(smallSize);
        multipleChunks = getQueryId(largeSize);

        try (val conn = getHyperQueryConnection()) {
            conn.waitFor(singleChunk, QueryStatus::allResultsProduced);
            conn.waitFor(multipleChunks, QueryStatus::allResultsProduced);
        }
    }

    @SneakyThrows
    @Test
    void canGetSimpleChunk() {
        val actual = sut(singleChunk, 0, 1);
        assertThat(actual).containsExactly(1, 2, 3, 4);
    }

    @SneakyThrows
    @Test
    void failsOnChunkOverrun() {
        assertThatThrownBy(() -> sut(singleChunk, 0, 2))
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("The requested chunk id '1' is out of range");
    }

    @SneakyThrows
    @Test
    void consecutiveChunksIncludeAllData() {

        val last = new AtomicLong(0);
        try (val connection = getHyperQueryConnection()) {
            val status = connection.waitFor(multipleChunks, QueryStatus::allResultsProduced);
            val rs = connection.getChunkBasedResultSet(multipleChunks, 0, status.getChunkCount());

            while (rs.next()) {
                assertThat(rs.getLong(1)).isEqualTo(last.incrementAndGet());
            }
        }

        assertThat(last.get()).isEqualTo(largeSize);
    }

    @SneakyThrows
    private static String getQueryId(int max) {
        val query = String.format(
                "select a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c, cast(a as numeric(38,18)) d from generate_series(1, %d) as s(a) order by a asc",
                max);

        try (val client = getHyperQueryConnection();
                val statement = client.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeQuery(query);
            return statement.getQueryId();
        }
    }
}
