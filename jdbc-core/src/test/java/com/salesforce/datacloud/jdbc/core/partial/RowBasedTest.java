/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.partial;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudPreparedStatement;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.query.v3.QueryStatus;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class RowBasedTest {
    @SneakyThrows
    private List<Integer> sut(String queryId, long offset, long limit) {
        try (val connection = getHyperQueryConnection()) {
            val resultSet = connection.getRowBasedResultSet(queryId, offset, limit);
            return toList(resultSet);
        }
    }

    private static final int tinySize = 8;
    private static final int smallSize = 64;

    private static String tiny;
    private static String small;

    @SneakyThrows
    @BeforeAll
    static void setupQueries() {
        small = getQueryId(smallSize);
        tiny = getQueryId(tinySize);
        try (val conn = getHyperQueryConnection()) {
            conn.waitFor(small, QueryStatus::allResultsProduced);
            conn.waitFor(tiny, QueryStatus::allResultsProduced);
        }
    }

    @SneakyThrows
    @Test
    void fetchWhereActualLessThanPageSize() {
        val limit = 10;

        assertThat(sut(small, 0, limit)).containsExactlyElementsOf(rangeClosed(1, 10));
        assertThat(sut(small, 10, limit)).containsExactlyElementsOf(rangeClosed(11, 20));
        assertThat(sut(small, 20, limit)).containsExactlyElementsOf(rangeClosed(21, 30));
        assertThat(sut(small, 30, 2)).containsExactlyElementsOf(rangeClosed(31, 32));
    }

    /**
     * DataCloudConnection::getRowBasedResultSet is not responsible for calculating the offset near the end of available rows
     */
    @SneakyThrows
    @Test
    void throwsWhenFullRangeOverrunsAvailableRows() {
        assertThatThrownBy(() -> sut(tiny, 0, tinySize * 3))
                .isInstanceOf(DataCloudJDBCException.class)
                .rootCause()
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining(String.format(
                        "INVALID_ARGUMENT: Request out of range: The specified offset is %d, but only %d tuples are available [TraceId:",
                        tinySize, tinySize));
    }

    static Stream<Arguments> querySizeAndPageSize() {
        val sizes = IntStream.rangeClosed(0, 10).mapToObj(i -> 1 << i).collect(Collectors.toList());
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
            val actual = pages.parallelStream()
                    .map(page -> {
                        try {
                            return conn.getRowBasedResultSet(queryId, page.getOffset(), page.getLimit());
                        } catch (DataCloudJDBCException e) {
                            throw new RuntimeException(e);
                        }
                    })
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
                    .map(page -> {
                        try {
                            return conn.getRowBasedResultSet(small, page.getOffset(), page.getLimit());
                        } catch (DataCloudJDBCException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .flatMap(RowBasedTest::toStream)
                    .collect(Collectors.toList());
            assertThat(actual).containsExactlyElementsOf(rangeClosed(1, smallSize));
        }
    }

    @SneakyThrows
    private long getRowCount(DataCloudConnection conn, String queryId) {
        return conn.waitFor(queryId, QueryStatus::allResultsProduced).getRowCount();
    }

    @SneakyThrows
    private static String getQueryId(int max) {
        val query = String.format(
                "select a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c, cast(a as numeric(38,18)) d from generate_series(1, %d) as s(a) order by a asc",
                max);

        try (val client = getHyperQueryConnection();
                val statement = client.createStatement().unwrap(DataCloudStatement.class)) {
            statement.executeAsyncQuery(query);
            return statement.getQueryId();
        }
    }

    private static List<Integer> rangeClosed(int start, int end) {
        return IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
    }

    public static Stream<Integer> toStream(DataCloudResultSet resultSet) {
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
