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

import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.util.RealisticArrowGenerator;
import com.salesforce.hyperdb.grpc.ExecuteQueryResponse;
import com.salesforce.hyperdb.grpc.QueryInfo;
import com.salesforce.hyperdb.grpc.QueryStatus;
import java.util.UUID;
import lombok.val;
import org.junit.jupiter.api.Test;

class AdaptiveQueryStatusPollerTest extends HyperGrpcTestBase {
    @Test
    void mapCapturesQueryStatus() {
        val id = UUID.randomUUID().toString();
        val sut = new AdaptiveQueryStatusPoller(id, hyperGrpcClient);
        assertThat(sut.pollQueryStatus()).isEqualTo(null);
        sut.map(null);
        assertThat(sut.pollQueryStatus()).isEqualTo(null);
        val status = QueryStatus.newBuilder()
                .setQueryId(UUID.randomUUID().toString())
                .setChunkCount(4L)
                .setCompletionStatus(QueryStatus.CompletionStatus.RESULTS_PRODUCED)
                .build();
        sut.map(ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder().setQueryStatus(status))
                .build());
        assertThat(sut.pollQueryStatus()).isEqualTo(status);
        sut.map(null);
        assertThat(sut.pollQueryStatus()).isEqualTo(status);
    }

    @Test
    void mapCapturesChunkCount() {
        val id = UUID.randomUUID().toString();
        val sut = new AdaptiveQueryStatusPoller(id, hyperGrpcClient);
        setupGetQueryInfo(id, QueryStatus.CompletionStatus.RESULTS_PRODUCED, 3);
        assertThat(sut.pollChunkCount()).isEqualTo(3L);
        sut.map(null);
        assertThat(sut.pollChunkCount()).isEqualTo(3L);
        sut.map(ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder()
                        .setQueryStatus(QueryStatus.newBuilder().setChunkCount(5L)))
                .build());
        assertThat(sut.pollChunkCount()).isEqualTo(3L);
        sut.map(null);
        assertThat(sut.pollChunkCount()).isEqualTo(3L);
    }

    @Test
    void mapCapturesCompletionStatus() {
        val id = UUID.randomUUID().toString();
        val sut = new AdaptiveQueryStatusPoller(id, hyperGrpcClient);
        setupGetQueryInfo(id, QueryStatus.CompletionStatus.RESULTS_PRODUCED, 3);
        assertThat(sut.pollChunkCount()).isEqualTo(3L);
    }

    @Test
    void pollChunkCountOnlyCallsQueryInfoIfUnfinished() {
        val id = UUID.randomUUID().toString();
        val sut = new AdaptiveQueryStatusPoller(id, hyperGrpcClient);
        sut.map(ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder()
                        .setQueryStatus(QueryStatus.newBuilder()
                                .setCompletionStatus(QueryStatus.CompletionStatus.RUNNING)
                                .build())
                        .build())
                .build());
        setupGetQueryInfo(id, QueryStatus.CompletionStatus.FINISHED, 3);
        assertThat(sut.pollChunkCount()).isEqualTo(3L);
        verifyGetQueryInfo(1);
    }

    @Test
    void pollChunkCountDoesNotCallQueryInfoIfFinished() {
        val id = UUID.randomUUID().toString();
        val sut = new AdaptiveQueryStatusPoller(id, hyperGrpcClient);
        sut.map(ExecuteQueryResponse.newBuilder()
                .setQueryInfo(QueryInfo.newBuilder()
                        .setQueryStatus(QueryStatus.newBuilder()
                                .setCompletionStatus(QueryStatus.CompletionStatus.FINISHED)
                                .setChunkCount(5L)
                                .build())
                        .build())
                .build());
        assertThat(sut.pollChunkCount()).isEqualTo(5L);
        verifyGetQueryInfo(0);
    }

    @Test
    void mapExtractsQueryResult() {
        val id = UUID.randomUUID().toString();
        val sut = new AdaptiveQueryStatusPoller(id, hyperGrpcClient);
        val data = RealisticArrowGenerator.data();
        val actual = sut.map(
                        ExecuteQueryResponse.newBuilder().setQueryResult(data).build())
                .orElseThrow(RuntimeException::new);
        assertThat(actual).isEqualTo(data);
    }
}
