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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.core.DataCloudQueryStatus;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
class RowBasedTest extends HyperTestBase {
    private List<Integer> sut(String queryId, long offset, long limit, RowBased.Mode mode) {
        val connection = getHyperQueryConnection();
        val resultSet = connection.getRowBasedResultSet(queryId, offset, limit, mode);
        return toList(resultSet);
    }

    private static final int tinySize = 8;
    private static final int smallSize = 32;
    private static final int largeSize = 1024 * 1024 * 10;

    private String tiny;
    private String small;
    private String large;

    @BeforeAll
    void setupQueries() {
        large = getQueryId(largeSize);
        small = getQueryId(smallSize);
        tiny = getQueryId(tinySize);
        waitForQuery(large);
        waitForQuery(small);
        waitForQuery(tiny);
    }

    @Test
    void singleRpcReturnsIteratorButNotRowBasedFullRange() {
        val client = mock(HyperGrpcClientExecutor.class);
        val single = RowBased.of(client, "select 1", 0, 1, RowBased.Mode.SINGLE_RPC);

        assertThat(single).isInstanceOf(RowBasedSingleRpc.class).isNotInstanceOf(RowBasedFullRange.class);
    }

    @Test
    void fullRangeReturnsRowBasedFullRange() {
        val client = mock(HyperGrpcClientExecutor.class);
        val single = RowBased.of(client, "select 1", 0, 1, RowBased.Mode.FULL_RANGE);

        assertThat(single).isInstanceOf(RowBasedFullRange.class).isNotInstanceOf(RowBasedSingleRpc.class);
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource(RowBased.Mode.class)
    void fetchWhereActualLessThanPageSize(RowBased.Mode mode) {
        val limit = 10;

        assertThat(sut(small, 0, limit, mode)).containsExactlyElementsOf(rangeClosed(1, 10));
        assertThat(sut(small, 10, limit, mode)).containsExactlyElementsOf(rangeClosed(11, 20));
        assertThat(sut(small, 20, limit, mode)).containsExactlyElementsOf(rangeClosed(21, 30));
        assertThat(sut(small, 30, 2, mode)).containsExactlyElementsOf(rangeClosed(31, 32));
    }

    @Test
    void fetchWhereActualMoreThanPageSize_SINGLE_RPC() {
        val actual = sut(small, 0, smallSize * 2, RowBased.Mode.SINGLE_RPC);
        assertThat(actual).isNotEmpty().isSubsetOf(rangeClosed(1, smallSize));
    }

    /**
     * DataCloudConnection::getRowBasedResultSet is not responsible for calculating the offset near the end of available rows
     */
    @SneakyThrows
    @Test
    void throwsWhenFullRangeOverrunsAvailableRows() {
        assertThatThrownBy(() -> sut(tiny, 0, tinySize * 3, RowBased.Mode.FULL_RANGE))
                .hasRootCauseInstanceOf(StatusRuntimeException.class)
                .hasRootCauseMessage(String.format(
                        "OUT_OF_RANGE: Request out of range: The specified offset is %d, but only %d tuples are available",
                        tinySize, tinySize));
    }

    Stream<Arguments> querySizeAndPageSize() {
        val sizes = IntStream.rangeClosed(0, 13).mapToObj(i -> 1 << i).collect(Collectors.toList());
        return sizes.stream().flatMap(left -> sizes.stream().map(right -> Arguments.of(left, right)));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("querySizeAndPageSize")
    void fullRangeRowBasedParameterizedQuery(int querySize, int limit) {
        val expected = rangeClosed(1, querySize);

        final String queryId;

        try (val conn = getHyperQueryConnection();
                val statement = conn.prepareStatement("select a from generate_series(1, ?) as s(a)")
                        .unwrap(DataCloudPreparedStatement.class)) {
            statement.setInt(1, querySize);
            statement.executeQuery();

            queryId = statement.getQueryId();
        }

        try (val conn = getHyperQueryConnection()) {
            val rows = getRowCount(conn, queryId);
            val pages = Page.stream(rows, limit).collect(Collectors.toList());
            log.info("pages: {}", pages);
            val actual = pages.parallelStream()
                    .map(page -> conn.getRowBasedResultSet(
                            queryId, page.getOffset(), page.getLimit(), RowBased.Mode.FULL_RANGE))
                    .flatMap(RowBasedTest::toStream)
                    .collect(Collectors.toList());
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    @SneakyThrows
    @Test
    void fetchWithRowsNearEndRange_FULL_RANGE() {
        try (val conn = getHyperQueryConnection()) {
            val rows = getRowCount(conn, small);
            val actual = Page.stream(rows, 5)
                    .parallel()
                    .map(page -> conn.getRowBasedResultSet(
                            small, page.getOffset(), page.getLimit(), RowBased.Mode.FULL_RANGE))
                    .flatMap(RowBasedTest::toStream)
                    .collect(Collectors.toList());
            assertThat(actual).containsExactlyElementsOf(rangeClosed(1, smallSize));
        }
    }

    private long getRowCount(DataCloudConnection conn, String queryId) {
        return conn.getQueryStatus(queryId)
                .filter(t -> t.isResultProduced() || t.isExecutionFinished())
                .map(DataCloudQueryStatus::getRowCount)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("boom"));
    }

    @SneakyThrows
    private String getQueryId(int max) {
        val query = String.format(
                "select a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c, cast(a as numeric(38,18)) d from generate_series(1, %d) as s(a) order by a asc",
                max);

        try (val client = getHyperQueryConnection();
                val statement = client.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeAsyncQuery(query);
            return statement.getQueryId();
        }
    }

    @SneakyThrows
    private void waitForQuery(String queryId) {
        try (val conn = getHyperQueryConnection()) {
            while (!isReady(conn, queryId)) {
                Thread.sleep(250);
            }
        }
    }

    private boolean isReady(DataCloudConnection connection, String queryId) {
        return connection.getQueryStatus(queryId).anyMatch(t -> t.isExecutionFinished() || t.isResultProduced());
    }

    private static List<Integer> rangeClosed(int start, int end) {
        return IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
    }

    private static Stream<Integer> toStream(DataCloudResultSet resultSet) {
        val iterator = new Iterator<Integer>() {
            @SneakyThrows
            @Override
            public boolean hasNext() {
                return resultSet.next();
            }

            @SneakyThrows
            @Override
            public Integer next() {
                return resultSet.getInt(1);
            }
        };

        return StreamUtilities.toStream(iterator);
    }

    private static List<Integer> toList(DataCloudResultSet resultSet) {

        return toStream(resultSet).collect(Collectors.toList());
    }
}
