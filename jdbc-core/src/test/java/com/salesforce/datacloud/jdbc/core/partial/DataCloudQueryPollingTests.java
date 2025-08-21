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
package com.salesforce.datacloud.jdbc.core.partial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;

@Slf4j
public class DataCloudQueryPollingTests extends HyperGrpcTestBase {
    ManagedChannel channel;
    HyperServiceGrpc.HyperServiceBlockingStub stub;

    @BeforeEach
    public void setup() {
        channel = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext()
                .build();
        stub = HyperServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    public void teardown() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    private static final String TEST_QUERY_ID = "test-query-123";
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(100);

    @Test
    public void whenPredicateSatisfiedImmediately_shouldReturnStatus() throws Exception {
        setupGetQueryInfo(
                TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 10);

        val deadline = Deadline.of(TEST_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, status -> status.getChunkCount() >= 5);

        val result = polling.waitFor();

        assertThat(result.allResultsProduced()).isFalse();
        assertThat(result.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RUNNING);
        verifyGetQueryInfo(1);
    }

    @Test
    public void whenQueryIsRunningThenFinishes_shouldReturnFinalStatus() throws Exception {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willReturn(Arrays.asList(
                        QueryInfo.newBuilder()
                                .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                        .setQueryId(TEST_QUERY_ID)
                                        .setCompletionStatus(
                                                salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus
                                                        .RUNNING_OR_UNSPECIFIED)
                                        .setChunkCount(1)
                                        .build())
                                .build(),
                        QueryInfo.newBuilder()
                                .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                        .setQueryId(TEST_QUERY_ID)
                                        .setCompletionStatus(
                                                salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                                        .setChunkCount(5)
                                        .build())
                                .build())));

        val deadline = Deadline.of(TEST_TIMEOUT);
        val stub = HyperServiceGrpc.newBlockingStub(InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext()
                .build());
        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, QueryStatus::allResultsProduced);

        val result = polling.waitFor();

        assertThat(result.allResultsProduced()).isTrue();
        assertThat(result.getChunkCount()).isEqualTo(5);
        verifyGetQueryInfo(1);
    }

    @Test
    public void whenCancelledExceptionThrown_shouldRetryGetQueryInfo() throws Exception {
        val callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    int count = callCount.incrementAndGet();
                    if (count == 1) {
                        observer.onError(Status.CANCELLED
                                .withDescription("Request cancelled")
                                .asRuntimeException());
                    } else {
                        observer.onNext(QueryInfo.newBuilder()
                                .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                        .setQueryId(TEST_QUERY_ID)
                                        .setCompletionStatus(
                                                salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                                        .setChunkCount(3)
                                        .build())
                                .build());
                        observer.onCompleted();
                    }
                }));

        val deadline = Deadline.of(TEST_TIMEOUT);
        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, QueryStatus::allResultsProduced);

        val result = polling.waitFor();

        assertThat(result.allResultsProduced()).isTrue();
        assertThat(result.getChunkCount()).isEqualTo(3);
        verifyGetQueryInfoAtLeast(2);
    }

    @Test
    public void whenStreamRunsOut_shouldRetryGetQueryInfoMultipleTimes() throws Exception {
        val callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    int count = callCount.incrementAndGet();
                    if (count < 3) {
                        observer.onCompleted();
                    } else {
                        observer.onNext(QueryInfo.newBuilder()
                                .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                        .setQueryId(TEST_QUERY_ID)
                                        .setCompletionStatus(
                                                salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                                        .setChunkCount(2)
                                        .build())
                                .build());
                        observer.onCompleted();
                    }
                }));

        val deadline = Deadline.of(TEST_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, QueryStatus::allResultsProduced);

        val result = polling.waitFor();

        assertThat(result.allResultsProduced()).isTrue();
        assertThat(result.getChunkCount()).isEqualTo(2);
        verifyGetQueryInfoAtLeast(3);
    }

    @Test
    public void whenTimeout_shouldThrowTimeoutException() {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willReturn(QueryInfo.newBuilder()
                        .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                .setQueryId(TEST_QUERY_ID)
                                .setCompletionStatus(
                                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED)
                                .setChunkCount(1)
                                .build())
                        .build()));

        val deadline = Deadline.of(SHORT_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, status -> false);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Predicate was not satisfied before timeout. queryId=test-query-123")
                .hasMessageContaining(TEST_QUERY_ID);

        verifyGetQueryInfoAtLeast(1);
    }

    @Test
    public void whenExecutionFinishedButPredicateNotSatisfied_shouldThrowException() {
        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 2);

        val deadline = Deadline.of(TEST_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, status -> status.getRowCount() >= 100);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasMessageContaining("Predicate was not satisfied when execution finished")
                .hasMessageContaining(TEST_QUERY_ID);

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenPredicateSatisfiedBeforeExecutionFinished_shouldReturnEarly() throws Exception {
        setupGetQueryInfo(
                TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 5);

        val deadline = Deadline.of(TEST_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, status -> status.getChunkCount() >= 3);

        val result = polling.waitFor();

        assertThat(result.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RUNNING);
        assertThat(result.getChunkCount()).isEqualTo(5);
        verifyGetQueryInfo(1);
    }

    @Test
    public void whenNonRetryableExceptionThrown_shouldPropagateException() {
        val message = "Invalid query ID " + UUID.randomUUID();
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    observer.onError(
                            Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
                }));

        val deadline = Deadline.of(TEST_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, QueryStatus::allResultsProduced);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(DataCloudJDBCException.class)
                .hasRootCauseMessage("INVALID_ARGUMENT: " + message)
                .hasRootCauseInstanceOf(StatusRuntimeException.class);

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenResultsProducedStatus_shouldReturnStatus() throws Exception {
        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED, 3);

        val deadline = Deadline.of(TEST_TIMEOUT);
        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, QueryStatus::allResultsProduced);

        val result = polling.waitFor();

        assertThat(result.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RESULTS_PRODUCED);
        assertThat(result.getChunkCount()).isEqualTo(3);
        assertThat(result.allResultsProduced()).isTrue();
        verifyGetQueryInfo(1);
    }

    @Test
    public void whenOptionalQueryInfoReceived_shouldSkipAndContinuePolling() throws Exception {
        val callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    int count = callCount.incrementAndGet();

                    observer.onNext(QueryInfo.newBuilder().setOptional(true).build());

                    observer.onNext(QueryInfo.newBuilder()
                            .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                    .setQueryId(TEST_QUERY_ID)
                                    .setCompletionStatus(
                                            salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                                    .setChunkCount(3)
                                    .build())
                            .build());
                    observer.onCompleted();
                }));

        val deadline = Deadline.of(TEST_TIMEOUT);
        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, QueryStatus::allResultsProduced);

        val result = polling.waitFor();

        assertThat(result.allResultsProduced()).isTrue();
        assertThat(result.getChunkCount()).isEqualTo(3);
        verifyGetQueryInfo(1);
    }

    @Test
    public void whenOnlyOptionalQueryInfoReceived_shouldRetryUntilTimeout() {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    observer.onNext(QueryInfo.newBuilder().setOptional(true).build());
                    observer.onCompleted();
                }));

        val deadline = Deadline.of(SHORT_TIMEOUT);

        val polling = DataCloudQueryPolling.of(stub, TEST_QUERY_ID, deadline, status -> false);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(DataCloudJDBCException.class)
                .satisfies(ex -> {
                    String msg = ex.getMessage();
                    boolean ok = msg.contains("Predicate was not satisfied before timeout.")
                            || msg.contains("Failed to get query status response.");
                    assertThat(ok).as("message contains timeout or failed-to-get").isTrue();
                });

        verifyGetQueryInfoAtLeast(2);
    }
}
