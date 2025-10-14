/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertEachRowIsTheSame;
import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;

@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class StreamingResultSetTest {
    public static String query(String arg) {
        return String.format(
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, %s) as s(a) order by a asc",
                arg);
    }

    private static final int rows = 64;
    private static final String regularSql = query(Integer.toString(rows));
    private static final String preparedSql = query("?");

    @SneakyThrows
    @Test
    public void testAdaptivePreparedStatement() {
        withPrepared(preparedSql, (conn, stmt) -> {
            val rs = stmt.executeQuery().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testAdaptiveStatement() {
        withStatement((conn, stmt) -> {
            val rs = stmt.executeQuery(regularSql).unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testAsyncPreparedStatement() {
        withPrepared(preparedSql, (conn, stmt) -> {
            stmt.executeAsyncQuery();
            conn.waitFor(stmt.getQueryId(), QueryStatus::allResultsProduced);
            val rs = stmt.getResultSet().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testAsyncStatement() {
        withStatement((conn, stmt) -> {
            stmt.executeAsyncQuery(regularSql);
            conn.waitFor(stmt.getQueryId(), QueryStatus::allResultsProduced);
            val rs = stmt.getResultSet().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    private void withStatement(ThrowingBiConsumer<DataCloudConnection, DataCloudStatement> func) {
        try (val conn = getHyperQueryConnection().unwrap(DataCloudConnection.class);
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            func.accept(conn, stmt);
        }
    }

    @SneakyThrows
    private void withPrepared(String sql, ThrowingBiConsumer<DataCloudConnection, DataCloudPreparedStatement> func) {
        try (val conn = getHyperQueryConnection().unwrap(DataCloudConnection.class);
                val stmt = conn.prepareStatement(sql).unwrap(DataCloudPreparedStatement.class)) {
            stmt.setInt(1, rows);
            func.accept(conn, stmt);
        }
    }

    @SneakyThrows
    private void assertThatResultSetIsCorrect(DataCloudConnection conn, DataCloudResultSet rs) {
        val witnessed = new AtomicInteger(0);

        assertThat(rs).isInstanceOf(StreamingResultSet.class);

        val status = conn.waitFor(rs.getQueryId(), QueryStatus::allResultsProduced);

        log.warn("Status: {}", status);

        assertThat(status).as("Status: " + status).satisfies(s -> {
            assertThat(s.allResultsProduced()).isTrue();
            assertThat(s.getRowCount()).isEqualTo(rows);
        });

        while (rs.next()) {
            assertEachRowIsTheSame(rs, witnessed);
            assertThat(rs.getRow()).isEqualTo(witnessed.get());
        }

        assertThat(witnessed.get())
                .as("last value seen from query: " + status.getQueryId())
                .isEqualTo(rows);
    }

    @SneakyThrows
    @Test
    public void testGetSchemaForQueryIdWithZeroResults() {
        withStatement((conn, stmt) -> {
            String sql =
                    "SELECT s, s::text as s_text, cast(s as numeric(38,18)) as s_numeric FROM generate_series(1,10) s LIMIT 0";

            final String queryId;
            try (DataCloudResultSet rs = stmt.executeQuery(sql).unwrap(DataCloudResultSet.class)) {
                queryId = rs.getQueryId();
            }

            ResultSetMetaData metaData = conn.getSchemaForQueryId(queryId);

            assertThat(metaData.getColumnCount()).as("column count").isEqualTo(3);

            assertThat(metaData.getColumnName(1)).as("integer column").isEqualTo("s");
            assertThat(metaData.getColumnType(1)).as("integer column").isEqualTo(Types.INTEGER);

            assertThat(metaData.getColumnName(2)).as("text column").isEqualTo("s_text");
            assertThat(metaData.getColumnType(2)).as("text column").isEqualTo(Types.VARCHAR);

            assertThat(metaData.getColumnName(3)).as("decimal column name").isEqualTo("s_numeric");
            assertThat(metaData.getColumnType(3)).as("decimal column type").isEqualTo(Types.DECIMAL);
            assertThat(metaData.getPrecision(3)).as("decimal column precision").isEqualTo(38);
            assertThat(metaData.getScale(3)).as("decimal column scale").isEqualTo(18);
        });
    }

    @SneakyThrows
    @Test
    public void testGetSchemaForQueryIdWithResults() {
        withStatement((conn, stmt) -> {
            String sql =
                    "SELECT s, s::text as s_text, cast(s as numeric(38,18)) as s_numeric FROM generate_series(1,3) s";

            final String queryId;
            try (DataCloudResultSet rs = stmt.executeQuery(sql).unwrap(DataCloudResultSet.class)) {
                queryId = rs.getQueryId();
                conn.waitFor(queryId, QueryStatus::allResultsProduced);
            }

            ResultSetMetaData metaData = conn.getSchemaForQueryId(queryId);

            assertThat(metaData.getColumnCount()).as("column count").isEqualTo(3);

            assertThat(metaData.getColumnName(1)).as("integer column").isEqualTo("s");
            assertThat(metaData.getColumnType(1)).as("integer column").isEqualTo(Types.INTEGER);

            assertThat(metaData.getColumnName(2)).as("text column").isEqualTo("s_text");
            assertThat(metaData.getColumnType(2)).as("text column").isEqualTo(Types.VARCHAR);

            assertThat(metaData.getColumnName(3)).as("decimal column name").isEqualTo("s_numeric");
            assertThat(metaData.getColumnType(3)).as("decimal column type").isEqualTo(Types.DECIMAL);
            assertThat(metaData.getPrecision(3)).as("decimal column precision").isEqualTo(38);
            assertThat(metaData.getScale(3)).as("decimal column scale").isEqualTo(18);
        });
    }

    @SneakyThrows
    @Test
    public void testGetSchemaForQueryIdWithInvalidQueryId() {
        withStatement((conn, stmt) -> {
            String invalidQueryId = "invalidQueryId";
            assertThat(assertThatThrownBy(() -> {
                        conn.getSchemaForQueryId(invalidQueryId);
                    })
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("The requested query ID is unknown"));
        });
    }

    @SneakyThrows
    @Test
    public void testGetSchemaForQueryIdWithNoSchemaData() {
        withStatement((conn, stmt) -> {
            String sql = "SELECT 1 as test_column";

            final String queryId;
            try (DataCloudResultSet rs = stmt.executeQuery(sql).unwrap(DataCloudResultSet.class)) {
                queryId = rs.getQueryId();
                conn.waitFor(queryId, QueryStatus::allResultsProduced);
            }
            try (MockedStatic<ProtocolMappers> mockedStatic = mockStatic(ProtocolMappers.class)) {
                mockedStatic
                        .when(() -> ProtocolMappers.fromQueryInfo(any()))
                        .thenReturn(java.util.Collections.emptyIterator());

                assertThat(assertThatThrownBy(() -> {
                            conn.getSchemaForQueryId(queryId);
                        })
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("Failed to fetch schema for queryId: " + queryId));
            }
        });
    }

    @FunctionalInterface
    interface ThrowingBiConsumer<T, U> {
        void accept(T var1, U var2) throws SQLException;
    }
}
