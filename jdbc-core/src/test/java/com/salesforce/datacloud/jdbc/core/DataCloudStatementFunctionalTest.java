/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithConnection;
import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStatement;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
public class DataCloudStatementFunctionalTest {
    private static final HyperServerConfig configWithSleep =
            HyperServerConfig.builder().build();

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
}
