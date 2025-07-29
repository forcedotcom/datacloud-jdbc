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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
public class AsyncStreamingResultSetTest {
    private static final int size = 64;

    private static final String sql = String.format(
            "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, %d) as s(a) order by a asc",
            size);

    @Test
    @SneakyThrows
    public void testThrowsOnNonsenseQueryAsync() {
        assertThatThrownBy(() -> {
                    try (val connection = getHyperQueryConnection();
                            val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
                        val rs = statement.executeAsyncQuery("select * from nonsense");
                        connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(5));
                        rs.getResultSet().next();
                    }
                })
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Failed to get query status response. queryId=")
                .hasCauseInstanceOf(StatusRuntimeException.class)
                .hasRootCauseMessage("FAILED_PRECONDITION: table \"nonsense\" does not exist");
    }

    @Test
    @SneakyThrows
    public void testNoDataIsLostAsync() {
        assertWithStatement(statement -> {
            statement.executeAsyncQuery(sql);

            val status = statement.connection.waitForResultsProduced(statement.getQueryId(), Duration.ofSeconds(30));

            val rs = statement.getResultSet();
            assertThat(status.allResultsProduced()).isTrue();
            assertThat(rs).isInstanceOf(StreamingResultSet.class);

            val expected = new AtomicInteger(0);

            while (rs.next()) {
                assertEachRowIsTheSame(rs, expected);
            }

            assertThat(expected.get()).isEqualTo(size);
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
