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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@ExtendWith(InProcessGrpcMockExtension.class)
class SyncQueryStatusListenerTest extends HyperGrpcTestBase {
    private final String query = "select * from stuff";
    private final QueryParam.TransferMode mode = QueryParam.TransferMode.SYNC;

    @SneakyThrows
    @Test
    void isReady() {
        setupExecuteQuery("", query, QueryParam.TransferMode.SYNC);
        val listener = SyncQueryStatusListener.of(query, hyperGrpcClient);
        QueryStatusListenerAssert.assertThat(listener).isReady();
    }

    @SneakyThrows
    @Test
    void getStatus() {
        val expected = Stream.of(QueryStatus.CompletionStatus.values())
                .filter(cs -> cs != QueryStatus.CompletionStatus.UNRECOGNIZED)
                .collect(Collectors.toList());
        val responses = expected.stream()
                .map(s -> ExecuteQueryResponse.newBuilder()
                        .setQueryInfo(QueryInfo.newBuilder()
                                .setQueryStatus(QueryStatus.newBuilder()
                                        .setCompletionStatus(s)
                                        .build())
                                .build())
                        .build())
                .collect(Collectors.toList());
        setupExecuteQuery("", query, QueryParam.TransferMode.SYNC, responses.toArray(new ExecuteQueryResponse[0]));
        val listener = SyncQueryStatusListener.of(query, hyperGrpcClient);
        assertThat(listener.getStatus()).isNull();
        val iterator = listener.stream().iterator();

        expected.forEach(exp -> {
            iterator.next();
            QueryStatusListenerAssert.assertThat(listener).hasStatus(exp.name());
        });
    }

    @SneakyThrows
    @Test
    void getQuery() {
        val randomQuery = this.query + UUID.randomUUID();
        val id = UUID.randomUUID().toString();
        setupExecuteQuery(id, randomQuery, QueryParam.TransferMode.SYNC);
        val listener = SyncQueryStatusListener.of(randomQuery, hyperGrpcClient);
        QueryStatusListenerAssert.assertThat(listener).hasQuery(randomQuery);
    }

    @SneakyThrows
    @Test
    void getQueryId() {
        val id = UUID.randomUUID().toString();
        setupExecuteQuery(id, query, QueryParam.TransferMode.SYNC);
        val listener = SyncQueryStatusListener.of(query, hyperGrpcClient);
        QueryStatusListenerAssert.assertThat(listener).hasQueryId(id);
    }

    @SneakyThrows
    @Test
    void getResultSet() {
        val id = UUID.randomUUID().toString();
        setupHyperGrpcClientWithMockedResultSet(id, ImmutableList.of());

        val listener = SyncQueryStatusListener.of(query, hyperGrpcClient);
        val resultSet = listener.generateResultSet();
        assertThat(resultSet).isNotNull();
        assertThat(resultSet.getQueryId()).isEqualTo(id);
    }
}
