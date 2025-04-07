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
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertEachRowIsTheSame;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.Constants;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class StreamingResultSetTest {
    public static String query(String arg) {
        return String.format(
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, %s) as s(a) order by a asc",
                arg);
    }

    private static final Properties none = new Properties();
    private static final int large = 10 * 1024 * 1024;
    private static final String regularSql = query(Integer.toString(large));
    private static final String preparedSql = query("?");

    @SneakyThrows
    @Test
    public void testAdaptivePreparedStatement() {
        withPrepared(none, preparedSql, (conn, stmt) -> {
            val rs = stmt.executeQuery().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testAdaptiveStatement() {
        withStatement(none, (conn, stmt) -> {
            val rs = stmt.executeQuery(regularSql).unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testAsyncPreparedStatement() {
        withPrepared(none, preparedSql, (conn, stmt) -> {
            stmt.executeAsyncQuery();
            conn.waitForResultsProduced(stmt.getQueryId(), Duration.ofSeconds(30));
            val rs = stmt.getResultSet().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testAsyncStatement() {
        withStatement(none, (conn, stmt) -> {
            stmt.executeAsyncQuery(regularSql);
            conn.waitForResultsProduced(stmt.getQueryId(), Duration.ofSeconds(30));
            val rs = stmt.getResultSet().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testSyncPreparedStatement() {
        withPrepared(sync(), preparedSql, (conn, stmt) -> {
            val rs = stmt.executeQuery().unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    @Test
    public void testSyncStatement() {
        withStatement(sync(), (conn, stmt) -> {
            val rs = stmt.executeQuery(regularSql).unwrap(DataCloudResultSet.class);
            assertThatResultSetIsCorrect(conn, rs);
        });
    }

    @SneakyThrows
    private void withStatement(
            Properties properties, ThrowingBiConsumer<DataCloudConnection, DataCloudStatement> func) {
        try (val conn = getHyperQueryConnection(properties).unwrap(DataCloudConnection.class);
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            func.accept(conn, stmt);
        }
    }

    @SneakyThrows
    private void withPrepared(
            Properties properties,
            String sql,
            ThrowingBiConsumer<DataCloudConnection, DataCloudPreparedStatement> func) {
        try (val conn = getHyperQueryConnection(properties).unwrap(DataCloudConnection.class);
                val stmt = conn.prepareStatement(sql).unwrap(DataCloudPreparedStatement.class)) {
            stmt.setInt(1, large);
            func.accept(conn, stmt);
        }
    }

    @SneakyThrows
    private void assertThatResultSetIsCorrect(DataCloudConnection conn, DataCloudResultSet rs) {
        val witnessed = new AtomicInteger(0);

        assertThat(rs).isInstanceOf(StreamingResultSet.class);

        val status = conn.waitForResultsProduced(rs.getQueryId(), Duration.ofSeconds(30));

        log.warn("Status: {}", status);

        assertThat(status).as("Status: " + status).satisfies(s -> {
            assertThat(s.allResultsProduced()).isTrue();
            assertThat(s.getRowCount()).isEqualTo(large);
        });

        assertThat(rs.isReady()).as("result set is ready").isTrue();

        while (rs.next()) {
            assertEachRowIsTheSame(rs, witnessed);
            assertThat(rs.getRow()).isEqualTo(witnessed.get());
        }

        assertThat(witnessed.get())
                .as("last value seen from query: " + status.getQueryId())
                .isEqualTo(large);
    }

    private static Properties sync() {
        val properties = new Properties();
        properties.put(Constants.FORCE_SYNC, true);
        return properties;
    }

    @FunctionalInterface
    interface ThrowingBiConsumer<T, U> {
        void accept(T var1, U var2) throws SQLException;
    }
}
