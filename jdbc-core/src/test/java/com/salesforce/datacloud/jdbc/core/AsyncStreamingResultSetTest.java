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
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
public class AsyncStreamingResultSetTest {

    private static final String sql =
            "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, 1024 * 1024 * 10) as s(a) order by a asc";

    @Test
    @SneakyThrows
    public void testThrowsOnNonsenseQueryAsync() {
        val ex = Assertions.assertThrows(DataCloudJDBCException.class, () -> {
            try (val connection = getHyperQueryConnection();
                    val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
                val rs = statement.executeAsyncQuery("select * from nonsense");
                connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(5));
                rs.getResultSet().next();
            }
        });

        AssertionsForClassTypes.assertThat(ex).hasCauseInstanceOf(StatusRuntimeException.class);
        val rootCausePattern = Pattern.compile("^[A-Z]+(_[A-Z]+)*: table \"nonsense\" does not exist.*");
        AssertionsForClassTypes.assertThat(ex.getCause().getMessage()).containsPattern(rootCausePattern);
    }

    @Test
    @SneakyThrows
    public void testNoDataIsLostAsync() {
        assertWithStatement(statement -> {
            statement.executeAsyncQuery(sql);

            val status = statement.dataCloudConnection.waitForResultsProduced(
                    statement.getQueryId(), Duration.ofSeconds(30));

            val rs = statement.getResultSet();
            assertThat(status.allResultsProduced()).isTrue();
            assertThat(rs).isInstanceOf(StreamingResultSet.class);

            val expected = new AtomicInteger(0);

            while (rs.next()) {
                assertEachRowIsTheSame(rs, expected);
            }

            assertThat(expected.get()).isEqualTo(1024 * 1024 * 10);
        });
    }

    @Test
    @SneakyThrows
    public void testQueryIdChangesInHeaderAsync() {
        try (val connection = getHyperQueryConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {

            val a = statement.executeAsyncQuery("select 4");
            val aQueryId = a.getQueryId();
            val b = statement.executeAsyncQuery("select 8");
            val bQueryId = b.getQueryId();

            assertThat(a).isSameAs(b);
            assertThat(aQueryId).isNotEqualTo(bQueryId);

            connection.waitForResultsProduced(bQueryId, Duration.ofSeconds(30));

            val rs = b.getResultSet();
            rs.next();

            assertThat(rs.getInt(1)).isEqualTo(8);
        } catch (StatusRuntimeException e) {
            Assertions.fail(e);
        }
    }
}
