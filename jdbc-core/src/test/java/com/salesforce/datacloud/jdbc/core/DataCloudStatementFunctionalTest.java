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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.HyperServerConfig;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.ResultSet;
import java.util.stream.Collectors;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithConnection;
import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@ExtendWith(HyperTestBase.class)
public class DataCloudStatementFunctionalTest {
    private static final HyperServerConfig configWithSleep =
            HyperServerConfig.builder().build();

    @Test
    @SneakyThrows
    public void canCancelStatementQuery() {
        try (val server = configWithSleep.start();
             val statement = server.getConnection().createStatement();
             val client = server.getRawClient()) {
            statement.execute("select pg_sleep(5000000);");

            val queryId = statement.unwrap(DataCloudStatement.class).getQueryId();
            val a = client.getQueryStatus(queryId).findFirst().get();
            assertThat(a.getCompletionStatus()).isEqualTo(DataCloudQueryStatus.CompletionStatus.RUNNING);

            statement.cancel();

            assertThatThrownBy(() -> client.getQueryStatus(queryId).collect(Collectors.toList()))
                    .hasMessage("FAILED_PRECONDITION: canceled");
        }
    }

    @Test
    @SneakyThrows
    public void canCancelPreparedStatementQuery() {
        try (val server = configWithSleep.start();
             val connection = server.getConnection();
                val statement = connection
                        .prepareStatement("select pg_sleep(?)")
                        .unwrap(DataCloudPreparedStatement.class);
             val client = server.getRawClient()) {

            statement.setInt(1, 5000000);
            statement.execute();

            val queryId = statement.getQueryId();
            val a = client.getQueryStatus(queryId).findFirst().get();
            assertThat(a.getCompletionStatus()).isEqualTo(DataCloudQueryStatus.CompletionStatus.RUNNING);

            statement.cancel();

            assertThatThrownBy(() -> client.getQueryStatus(queryId).collect(Collectors.toList()))
                    .hasMessage("FAILED_PRECONDITION: canceled");
        }
    }

    @Test
    @SneakyThrows
    public void canCancelAnotherQueryById() {
        try (val server = configWithSleep.start();
             val connection = server.getConnection().unwrap(DataCloudConnection.class);
             val statement = connection.createStatement().unwrap(DataCloudStatement.class);
             val client = server.getRawClient()) {

            statement.execute("select pg_sleep(5000000);");
            val queryId = statement.getQueryId();

            val a = client.getQueryStatus(queryId).findFirst().get();
            assertThat(a.getCompletionStatus()).isEqualTo(DataCloudQueryStatus.CompletionStatus.RUNNING);

            connection.cancel(queryId);

            assertThatThrownBy(() -> client.getQueryStatus(queryId).collect(Collectors.toList()))
                    .hasMessage("FAILED_PRECONDITION: canceled");
        }
    }

    @Test
    @SneakyThrows
    public void noErrorOnCancelUnknownQuery() {
        assertWithConnection(connection -> connection.cancel("nonsense query id"));
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
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(EXECUTED_MESSAGE));

        assertWithStatement(statement -> assertThatThrownBy(statement::getResultSetConcurrency)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(EXECUTED_MESSAGE));

        assertWithStatement(statement -> assertThatThrownBy(statement::getFetchDirection)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessage(EXECUTED_MESSAGE));
    }
}
