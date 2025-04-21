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
package com.salesforce.datacloud.jdbc.core.listener;

import static com.salesforce.datacloud.jdbc.core.DataCloudConnectionMocker.mockedConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.RealisticArrowGenerator;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@ExtendWith(InProcessGrpcMockExtension.class)
class AsyncQueryStatusListenerTest extends HyperGrpcTestBase {
    private final String query = "select * from stuff";
    private final QueryParam.TransferMode mode = QueryParam.TransferMode.ASYNC;

    @SneakyThrows
    @ParameterizedTest
    @CsvSource({"0, RUNNING", "1, RESULTS_PRODUCED", "2, FINISHED"})
    void itCanGetStatus(int value, String expected) {
        val queryId = UUID.randomUUID().toString();
        setupExecuteQuery(queryId, query, mode);
        val listener = sut(query);

        setupGetQueryInfo(queryId, QueryStatus.CompletionStatus.forNumber(value));
        assertThat(listener.getStatus()).isEqualTo(expected);
    }

    @Test
    void itThrowsIfStreamIsRequestedBeforeReady() {
        val queryId = UUID.randomUUID().toString();

        setupExecuteQuery(queryId, query, mode);
        val listener = sut(query);

        setupGetQueryInfo(queryId, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);

        QueryStatusListenerAssert.assertThat(listener).isNotReady();
        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class, () -> listener.stream().collect(Collectors.toList()));
        AssertionsForClassTypes.assertThat(ex).hasMessageContaining(QueryStatusListener.BEFORE_READY);
    }

    @SneakyThrows
    @Test
    void itCorrectlyReturnsStreamOfParts() {
        val queryId = UUID.randomUUID().toString();
        setupExecuteQuery(queryId, query, mode);
        setupGetQueryInfo(queryId, QueryStatus.CompletionStatus.FINISHED, 2);
        val twoStudents = ImmutableList.of(
                new RealisticArrowGenerator.Student(1, "alice", 2), new RealisticArrowGenerator.Student(2, "bob", 3));
        setupGetQueryResult(queryId, 0, 2, twoStudents);
        setupGetQueryResult(queryId, 1, 2, twoStudents);

        val iterator = sut(query).stream().iterator();

        assertThat(iterator)
                .as("Retrieves status under the hood so we now know there are two chunks on the server")
                .hasNext();
        iterator.next();
        assertThat(iterator)
                .as("There should be an additional QueryResult in memory and one chunk left on the server")
                .hasNext();
        iterator.next();
        assertThat(iterator)
                .as("We've exhausted our in-memory QueryResults but there's still a chunk left on the server")
                .hasNext();
        iterator.next();
        assertThat(iterator).as("There's still one QueryResult left in memory").hasNext();
        iterator.next();
        assertThat(iterator)
                .as("We've already used up all the QueryResults in memory and there are no remaining chunks")
                .isExhausted();

        Assertions.assertThrows(NoSuchElementException.class, iterator::next);
    }

    @SneakyThrows
    @Test
    public void itCorrectlyReturnsResultSet() {
        val random = new Random(10);
        val queryId = UUID.randomUUID().toString();
        val studentId = random.nextInt();
        val studentGrade = random.nextDouble();
        val studentName = UUID.randomUUID().toString();
        setupHyperGrpcClientWithMockedResultSet(queryId, ImmutableList.of());
        setupGetQueryInfo(queryId, QueryStatus.CompletionStatus.FINISHED);
        setupGetQueryResult(
                queryId,
                0,
                1,
                ImmutableList.of(new RealisticArrowGenerator.Student(studentId, studentName, studentGrade)));
        val resultSet = sut(query).generateResultSet();
        assertThat(resultSet).isNotNull();
        assertThat(resultSet.getQueryId()).isEqualTo(queryId);
        assertThat(resultSet.isReady()).isTrue();

        resultSet.next();
        assertThat(resultSet.getInt(1)).isEqualTo(studentId);
        assertThat(resultSet.getString(2)).isEqualTo(studentName);
        assertThat(resultSet.getDouble(3)).isEqualTo(studentGrade);
    }

    @SneakyThrows
    @Test
    void userShouldExecuteQueryBeforeAccessingResultSet() {
        try (val statement = statement()) {
            val ex = assertThrows(DataCloudJDBCException.class, statement::getResultSet);
            AssertionsForClassTypes.assertThat(ex)
                    .hasMessageContaining("a query was not executed before attempting to access results");
        }
    }

    @SneakyThrows
    @Test
    void userShouldWaitForQueryBeforeAccessingResultSet() {
        val queryId = UUID.randomUUID().toString();
        setupHyperGrpcClientWithMockedResultSet(queryId, ImmutableList.of());
        setupGetQueryInfo(queryId, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);

        try (val statement = statement().executeAsyncQuery(query)) {
            val ex = assertThrows(DataCloudJDBCException.class, statement::getResultSet);
            AssertionsForClassTypes.assertThat(ex).hasMessageContaining("query results were not ready");
        }
    }

    DataCloudStatement statement() {
        val connection = mockedConnection(new Properties(), hyperGrpcClient);
        return new DataCloudStatement(connection);
    }

    @SneakyThrows
    QueryStatusListener sut(String query) {
        return AsyncQueryStatusListener.of(query, hyperGrpcClient, Duration.ofSeconds(5));
    }
}
