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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.jdbc.util.ThrowingBiFunction;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class StreamingResultSetTest extends HyperTestBase {
    private static final int small = 10;
    private static final int large = 10 * 1024 * 1024;

    private static Stream<Arguments> queryModes(int size) {
        return Stream.of(
                inline("executeSyncQuery", DataCloudStatement::executeSyncQuery, size),
                inline("executeAdaptiveQuery", DataCloudStatement::executeAdaptiveQuery, size),
                deferred("executeAsyncQuery", DataCloudStatement::executeAsyncQuery, true, size),
                deferred("execute", DataCloudStatement::execute, false, size),
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
        val expected = new AtomicInteger(0);

        assertWithConnection(conn -> {
            try (val statement = conn.prepareStatement(sql)) {
                statement.setInt(1, large);

                val rs = statement.executeQuery();
                assertThat(rs).isInstanceOf(StreamingResultSet.class);
                assertThat(((StreamingResultSet) rs).isReady()).isTrue();

                while (rs.next()) {
                    assertEachRowIsTheSame(rs, expected);
                }
            }
        });

        assertThat(expected.get()).isEqualTo(large);
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
        return arguments(named(String.format("%s -> DataCloudResultSet", name), impl), size);
    }

    private static Arguments deferred(
            String name, ThrowingBiFunction<DataCloudStatement, String, Object> impl, Boolean wait, int size) {
        ThrowingBiFunction<DataCloudStatement, String, DataCloudResultSet> deferred =
                (DataCloudStatement s, String x) -> {
                    impl.apply(s, x);

                    if (wait) {
                        waitUntilReady(s);
                    }

                    return (DataCloudResultSet) s.getResultSet();
                };
        return arguments(named(String.format("%s; getResultSet -> DataCloudResultSet", name), deferred), size);
    }

    @SneakyThrows
    static boolean waitUntilReady(DataCloudStatement statement) {
        while (!statement.isReady()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }
}
