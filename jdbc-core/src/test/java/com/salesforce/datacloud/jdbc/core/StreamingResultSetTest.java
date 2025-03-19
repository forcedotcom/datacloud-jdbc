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

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.ThrowingBiFunction;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertEachRowIsTheSame;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithConnection;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
@ExtendWith(HyperTestBase.class)
public class StreamingResultSetTest {
    private static final int small = 10;
    private static final int large = 10 * 1024 * 1024;

    private static Stream<Arguments> queryModes(int size) {
        return Stream.of(
                inline("executeSyncQuery", DataCloudStatement::executeSyncQuery, size),
                deferred("execute", DataCloudStatement::execute, true, size),
                deferred("executeQuery", DataCloudStatement::executeQuery, false, size));
    }

    public static Stream<Arguments> queryModesWithMax() {
        return Stream.of(small, large).flatMap(StreamingResultSetTest::queryModes);
    }

    @SneakyThrows
    @Test
    public void exercisePreparedStatement() {
        val sql =
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, ?) as s(a) order by a asc";
        val witnessed = new AtomicInteger(0);

        assertWithConnection(conn -> {
            try (val statement = conn.prepareStatement(sql).unwrap(DataCloudPreparedStatement.class)) {
                statement.setInt(1, large);

                val rs = statement.executeQuery();

                val status = statement.getConnection().unwrap(DataCloudConnection.class).waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30));
                log.warn("Status: {}", status);

                assertThat(status).as("Status: " + status).satisfies(s -> {
                    assertThat(s.allResultsProduced()).isTrue();
                    assertThat(s.getRowCount()).isEqualTo(large);
                });

                assertThat(rs).isInstanceOf(StreamingResultSet.class);
                assertThat(((StreamingResultSet) rs).isReady()).isTrue();

                while (rs.next()) {
                    assertEachRowIsTheSame(rs, witnessed);
                    assertThat(rs.getRow()).isEqualTo(witnessed.get());
                }
            }
        });

        assertThat(witnessed.get()).isEqualTo(large);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("queryModesWithMax")
    public void exerciseQueryMode(
            ThrowingBiFunction<DataCloudStatement, String, DataCloudResultSet> queryMode, int max) {
        val sql = query(max);
        val actual = new AtomicInteger(0);

        assertWithStatement(statement -> {
            val rs = queryMode.apply(statement, sql);

            val status = statement.getConnection().unwrap(DataCloudConnection.class).waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30));
            log.warn("Status: {}", status);

            assertThat(status).as("Status: " + status).satisfies(s -> {
                assertThat(s.allResultsProduced()).isTrue();
                assertThat(s.getRowCount()).isEqualTo(max);
            });

            assertThat(rs).isInstanceOf(StreamingResultSet.class);
            assertThat(rs.isReady()).isTrue();

            while (rs.next()) {
                assertEachRowIsTheSame(rs, actual);
            }
        });

        assertThat(actual.get()).isEqualTo(max);
    }

    private static Stream<Arguments> queryModesWithNoSize() {
        return queryModes(-1);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("queryModesWithNoSize")
    public void allModesThrowOnNonsense(ThrowingBiFunction<DataCloudStatement, String, DataCloudResultSet> queryMode) {
        val ex = Assertions.assertThrows(SQLException.class, () -> {
            try (val conn = getHyperQueryConnection();
                    val statement = (DataCloudStatement) conn.createStatement()) {
                val result = queryMode.apply(statement, "select * from nonsense");
                result.next();
            }
        });

        AssertionsForClassTypes.assertThat(ex).hasRootCauseInstanceOf(StatusRuntimeException.class);
    }

    public static String query(int max) {
        return String.format(
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, %d) as s(a) order by a asc",
                max);
    }

    private static Arguments inline(
            String name, ThrowingBiFunction<DataCloudStatement, String, DataCloudResultSet> impl, int size) {
        log.warn("testing: {}", name);
        return arguments(named(String.format("%s -> DataCloudResultSet", name), impl), size);
    }

    private static Arguments deferred(
            String name, ThrowingBiFunction<DataCloudStatement, String, Object> impl, Boolean wait, int size) {
        log.warn("testing: {}", name);
        ThrowingBiFunction<DataCloudStatement, String, DataCloudResultSet> deferred =
                (DataCloudStatement s, String x) -> {
                    impl.apply(s, x);

                    if (wait) {
                        val conn = s.getConnection().unwrap(DataCloudConnection.class);
                        conn.waitForResultsProduced(s.getQueryId(), Duration.ofSeconds(30));
                    }

                    return (DataCloudResultSet) s.getResultSet();
                };
        return arguments(named(String.format("%s; getResultSet -> DataCloudResultSet", name), deferred), size);
    }

}
