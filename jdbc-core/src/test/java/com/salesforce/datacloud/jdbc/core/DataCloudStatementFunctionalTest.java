/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithConnection;
import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStatement;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.RowRangeIterator;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(LocalHyperTestBase.class)
public class DataCloudStatementFunctionalTest {
    private static final HyperServerConfig configWithSleep =
            HyperServerConfig.builder().build();

    private static void acceptThrows(DataCloudStatement statement) {
        val ex = assertThrows(DataCloudJDBCException.class, () -> statement.executeQuery("SELECT a"));
        assertThat(ex)
                .hasMessageContaining("is not supported in Data Cloud query")
                .hasFieldOrPropertyWithValue("SQLState", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Test
    @SneakyThrows
    public void canCancelStatementQuery() {
        try (val server = configWithSleep.start();
                val conn = server.getConnection();
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            stmt.executeAsyncQuery("select pg_sleep(5000000);");

            val queryId = stmt.unwrap(DataCloudStatement.class).getQueryId();
            val a = conn.waitFor(queryId, t -> true);
            assertThat(a.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RUNNING);

            stmt.cancel();
            assertThatThrownBy(() -> {
                        conn.waitFor(queryId, QueryStatus::allResultsProduced);
                    })
                    .hasMessageContaining("Failed to execute query: canceled by user")
                    .hasMessageContaining("SQLSTATE: 57014");
        }
    }

    @Test
    @SneakyThrows
    public void canCancelPreparedStatementQuery() {
        try (val server = configWithSleep.start();
                val conn = server.getConnection();
                val stmt = conn.prepareStatement("select pg_sleep(?)").unwrap(DataCloudPreparedStatement.class)) {
            stmt.setInt(1, 5000000);
            stmt.executeAsyncQuery();

            val queryId = stmt.getQueryId();
            // Wait for at least one query info message to ensure query is running
            val status = conn.waitFor(queryId, t -> true);
            assertThat(status.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RUNNING);

            stmt.cancel();
            assertThatThrownBy(() -> {
                        conn.waitFor(queryId, QueryStatus::allResultsProduced);
                    })
                    .hasMessageContaining("Failed to execute query: canceled by user")
                    .hasMessageContaining("SQLSTATE: 57014");
        }
    }

    @Test
    @SneakyThrows
    public void canCancelAnotherQueryById() {
        try (val server = configWithSleep.start();
                val conn = server.getConnection().unwrap(DataCloudConnection.class);
                val stmt = conn.createStatement().unwrap(DataCloudStatement.class)) {
            stmt.executeAsyncQuery("select pg_sleep(5000000);");
            val queryId = stmt.getQueryId();

            // Wait for at least one query info message to ensure query is running
            val status = conn.waitFor(queryId, t -> true);
            assertThat(status.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RUNNING);

            conn.cancelQuery(queryId);
            assertThatThrownBy(() -> {
                        conn.waitFor(queryId, QueryStatus::allResultsProduced);
                    })
                    .hasMessageContaining("Failed to execute query: canceled by user")
                    .hasMessageContaining("SQLSTATE: 57014");
        }
    }

    @Test
    @SneakyThrows
    public void noErrorOnCancelUnknownQuery() {
        assertWithConnection(connection -> connection.cancelQuery("nonsense query id"));
    }

    @Test
    @SneakyThrows
    public void forwardAndReadOnly() {
        assertWithStatement(statement -> {
            val rs = statement.executeQuery("select 1");

            assertThat(statement.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            assertThat(statement.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
            assertThat(statement.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);

            assertThat(rs.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(rs.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);

            assertThat(rs.getRow()).isEqualTo(0);
        });
    }

    private static final String EXECUTED_MESSAGE = "a query was not executed before attempting to access results";

    @SneakyThrows
    @Test
    public void requiresExecutedResultSet() {
        assertWithStatement(statement -> assertThatThrownBy(statement::getResultSetType)
                .isInstanceOf(SQLException.class)
                .hasMessage(EXECUTED_MESSAGE));

        assertWithStatement(statement -> assertThatThrownBy(statement::getResultSetConcurrency)
                .isInstanceOf(SQLException.class)
                .hasMessage(EXECUTED_MESSAGE));

        assertWithStatement(statement -> assertThatThrownBy(statement::getFetchDirection)
                .isInstanceOf(SQLException.class)
                .hasMessage(EXECUTED_MESSAGE));
    }

    @Test
    @SneakyThrows
    public void testExecuteQuery() {
        assertWithStatement(statement -> {
            ResultSet response = statement.executeQuery("SELECT 1 as id, 2 as name, 3 as grade");
            assertNotNull(response);
            assertThat(response.getMetaData().getColumnCount()).isEqualTo(3);
            assertThat(response.getMetaData().getColumnName(1)).isEqualTo("id");
            assertThat(response.getMetaData().getColumnName(2)).isEqualTo("name");
            assertThat(response.getMetaData().getColumnName(3)).isEqualTo("grade");
        });
    }

    @Test
    @SneakyThrows
    public void testExecute() {
        assertWithStatement(statement -> {
            statement.execute(
                    "SELECT md5(random()::text) AS id, md5(random()::text) AS name, round((random() * 3 + 1)::numeric, 2) AS grade FROM generate_series(1, 3);");
            val response = statement.getResultSet();
            assertNotNull(response);
            assertThat(response.getMetaData().getColumnCount()).isEqualTo(3);
            assertThat(response.getMetaData().getColumnName(1)).isEqualTo("id");
            assertThat(response.getMetaData().getColumnName(2)).isEqualTo("name");
            assertThat(response.getMetaData().getColumnName(3)).isEqualTo("grade");
        });
    }

    @Test
    public void testExecuteQueryWithSqlException() {
        assertWithStatement(statement -> {
            val ex = assertThrows(DataCloudJDBCException.class, () -> statement.executeQuery("SELECT a"));
            assertThat(ex)
                    .hasMessageContaining("Failed to execute query: unknown column 'a'")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            // Also test that getResultSet throws same error
            val ex2 = assertThrows(DataCloudJDBCException.class, statement::getResultSet);
            assertEquals(ex.getPrimaryMessage(), ex2.getPrimaryMessage());
            assertEquals(ex.getSQLState(), ex2.getSQLState());
        });
    }

    @Test
    public void testExecuteUpdate() {
        assertWithStatement(statement -> {
            String sql = "UPDATE table SET column = value";
            val e = assertThrows(SQLException.class, () -> statement.executeUpdate(sql));
            assertThat(e)
                    .hasMessageContaining("is not supported in Data Cloud query")
                    .hasFieldOrPropertyWithValue("SQLState", "0A000");
        });
    }

    @Test
    public void testSetQueryTimeoutNegativeValue() {
        assertWithStatement(statement -> {
            statement.setQueryTimeout(-100);
            assertThat(statement.getQueryTimeout()).isEqualTo(0);
        });
    }

    @Test
    public void testGetQueryTimeoutDefaultValue() {
        assertWithStatement(statement -> {
            assertThat(statement.getQueryTimeout()).isEqualTo(0);
        });
    }

    @Test
    public void testGetQueryTimeoutSetInQueryStatementLevel() {
        assertWithStatement(statement -> {
            statement.setQueryTimeout(10);
            assertThat(statement.getQueryTimeout()).isEqualTo(10);
        });
    }

    @Test
    @SneakyThrows
    public void testCloseIsNullSafe() {
        assertWithStatement(DataCloudStatement::close);
    }

    @ParameterizedTest
    @SneakyThrows
    @ValueSource(
            ints = {
                RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE + 1,
                RowRangeIterator.HYPER_MIN_ROW_LIMIT_BYTE_SIZE - 1,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE
            })
    public void testConstraintsInvalid(int bytes) {
        assertWithStatement(statement -> {
            assertThatThrownBy(() -> statement.setResultSetConstraints(0, bytes))
                    .hasMessageContaining(
                            "The specified maxBytes (%d) must satisfy the following constraints: %d >= x >= %d",
                            bytes,
                            RowRangeIterator.HYPER_MIN_ROW_LIMIT_BYTE_SIZE,
                            RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE);
        });
    }

    @ParameterizedTest
    @SneakyThrows
    @ValueSource(
            ints = {
                RowRangeIterator.HYPER_MAX_ROW_LIMIT_BYTE_SIZE,
                RowRangeIterator.HYPER_MIN_ROW_LIMIT_BYTE_SIZE,
                100000
            })
    public void testConstraintsValid(int bytes) {
        assertWithStatement(statement -> {
            val rows = 123 + bytes;
            statement.setResultSetConstraints(rows, bytes);
            assertThat(statement.getTargetMaxBytes()).isEqualTo(bytes);
            assertThat(statement.getTargetMaxRows()).isEqualTo(rows);
            statement.clearResultSetConstraints();
            assertThat(statement.getTargetMaxBytes()).isEqualTo(0);
            assertThat(statement.getTargetMaxRows()).isEqualTo(0);
        });
    }

    @SneakyThrows
    @Test
    public void testConstraintsDefaults() {
        assertWithStatement(stmt -> {
            assertThat(stmt.getTargetMaxBytes()).isEqualTo(0);
            assertThat(stmt.getTargetMaxRows()).isEqualTo(0);
        });
    }
}
