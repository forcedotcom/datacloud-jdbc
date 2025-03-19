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

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.RealisticArrowGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.AssertionsForClassTypes;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith(MockitoExtension.class)
@ExtendWith(InProcessGrpcMockExtension.class)
public class AdaptiveQueryStatusListenerTest extends HyperGrpcTestBase {
    private final String query = "select * from stuff";
    private final QueryParam.TransferMode mode = QueryParam.TransferMode.ADAPTIVE;

    private final RealisticArrowGenerator.Student alice = new RealisticArrowGenerator.Student(1, "alice", 2);
    private final RealisticArrowGenerator.Student bob = new RealisticArrowGenerator.Student(2, "bob", 3);

    private final List<RealisticArrowGenerator.Student> twoStudents = ImmutableList.of(alice, bob);

    @SneakyThrows
    @Test
    void itWillWaitUntilResultsProducedOrFinishedToProduceStatus() {
        val queryId = UUID.randomUUID().toString();
        setupExecuteQuery(queryId, query, mode, executeQueryResponse(queryId, null, 1));
        setupGetQueryInfo(queryId, QueryStatus.CompletionStatus.RESULTS_PRODUCED, 3);
        val listener = sut(query);
        QueryStatusListenerAssert.assertThat(listener).hasStatus(QueryStatus.CompletionStatus.RESULTS_PRODUCED.name());
    }

    @SneakyThrows
    @Test
    void itReturnsNoChunkResults() {
        val queryId = UUID.randomUUID().toString();
        setupExecuteQuery(
                queryId,
                query,
                mode,
                executeQueryResponseWithData(twoStudents),
                executeQueryResponse(queryId, QueryStatus.CompletionStatus.RESULTS_PRODUCED, 1));

        val resultSet = sut(query).generateResultSet();

        assertThat(resultSet).isNotNull();
        assertThat(resultSet.getQueryId()).isEqualTo(queryId);
        assertThat(resultSet.isReady()).isTrue();

        resultSet.next();
        assertThat(resultSet.getInt("id")).isEqualTo(alice.getId());
        assertThat(resultSet.getString("name")).isEqualTo(alice.getName());
        assertThat(resultSet.getDouble("grade")).isEqualTo(alice.getGrade());

        resultSet.next();
        assertThat(resultSet.getInt("id")).isEqualTo(bob.getId());
        assertThat(resultSet.getString("name")).isEqualTo(bob.getName());
        assertThat(resultSet.getDouble("grade")).isEqualTo(bob.getGrade());
    }

    @SneakyThrows
    @Test
    void itIsAlwaysReadyBecauseWeImmediatelyGetResultsThenBlockForAsyncIfNecessary() {
        val queryId = UUID.randomUUID().toString();
        setupExecuteQuery(queryId, query, mode, executeQueryResponse(queryId, null, 1));
        val listener = sut(query);

        assertThat(listener.isReady()).isTrue();
    }

    @SneakyThrows
    @Test
    void itCatchesAndMakesSqlExceptionWhenQueryFails() {
        val client = Mockito.mock(HyperGrpcClientExecutor.class);
        Mockito.when(client.executeAdaptiveQuery(Mockito.anyString()))
                .thenThrow(new StatusRuntimeException(Status.ABORTED));

        val ex = Assertions.assertThrows(
                DataCloudJDBCException.class, () -> AdaptiveQueryStatusListener.of("any", client, Duration.ZERO));
        AssertionsForClassTypes.assertThat(ex)
                .hasMessageContaining("Failed to execute query: ")
                .hasRootCauseInstanceOf(StatusRuntimeException.class);
    }

    @SneakyThrows
    QueryStatusListener sut(String query) {
        return AdaptiveQueryStatusListener.of(query, hyperGrpcClient, Duration.ofMinutes(1));
    }
}
