/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertEachRowIsTheSame;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.StatusRuntimeException;
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
                        statement.executeAsyncQuery("select * from nonsense");
                        connection.waitFor(statement.getQueryId(), QueryStatus::allResultsProduced);
                    }
                })
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("HINT:")
                .hasMessageContaining("42P01: table \"nonsense\" does not exist")
                .hasCauseInstanceOf(StatusRuntimeException.class)
                .hasRootCauseMessage("FAILED_PRECONDITION: table \"nonsense\" does not exist");
    }

    @Test
    @SneakyThrows
    public void testNoDataIsLostAsync() {
        assertWithStatement(statement -> {
            statement.executeAsyncQuery(sql);
            val status = statement.connection.waitFor(statement.getQueryId(), QueryStatus::allResultsProduced);

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

            connection.waitFor(statement.getQueryId(), QueryStatus::allResultsProduced);

            val rs = b.getResultSet();
            rs.next();

            assertThat(rs.getInt(1)).isEqualTo(8);
        } catch (StatusRuntimeException e) {
            Assertions.fail(e);
        }
    }
}
