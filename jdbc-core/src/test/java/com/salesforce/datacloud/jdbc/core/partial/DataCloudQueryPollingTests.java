/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.partial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;

@Slf4j
public class DataCloudQueryPollingTests extends InterceptedHyperTestBase {
    ManagedChannel channel;
    QueryAccessGrpcClient queryClient;

    @BeforeEach
    public void setup() {
        channel = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext()
                .build();
        queryClient = QueryAccessGrpcClient.of(TEST_QUERY_ID, HyperServiceGrpc.newStub(channel));
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

    private QueryAccessGrpcClient getQueryClientWithTimeout(Duration duration) {
        return queryClient.withStubConfiguration(
                stub -> stub.withDeadlineAfter(duration.toMillis(), TimeUnit.MILLISECONDS));
    }

    @Test
    public void whenPredicateSatisfiedImmediately_shouldReturnStatus() throws Exception {
        setupGetQueryInfo(
                TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 10);

        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, status -> status.getChunkCount() >= 5);
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
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, QueryStatus::allResultsProduced);

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
                    if (count < 2) {
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

        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, QueryStatus::allResultsProduced);

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
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, QueryStatus::allResultsProduced);

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
        val polling = DataCloudQueryPolling.of(getQueryClientWithTimeout(SHORT_TIMEOUT), true, status -> false);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Predicate was not satisfied before timeout. queryId=test-query-123")
                .hasMessageContaining(TEST_QUERY_ID);

        verifyGetQueryInfoAtLeast(1);
    }

    @Test
    public void whenExecutionFinishedButPredicateNotSatisfied_shouldThrowException() {
        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 2);
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, status -> status.getRowCount() >= 100);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Predicate was not satisfied when execution finished")
                .hasMessageContaining(TEST_QUERY_ID);

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenPredicateSatisfiedBeforeExecutionFinished_shouldReturnEarly() throws Exception {
        setupGetQueryInfo(
                TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 5);
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, status -> status.getChunkCount() >= 3);

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
                .willProxyTo((request, observer) -> observer.onError(
                        Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException())));
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, QueryStatus::allResultsProduced);

        assertThatThrownBy(polling::waitFor)
                .isInstanceOf(SQLException.class)
                .hasRootCauseMessage("INVALID_ARGUMENT: " + message)
                .hasRootCauseInstanceOf(StatusRuntimeException.class);

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenResultsProducedStatus_shouldReturnStatus() throws Exception {
        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED, 3);
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, QueryStatus::allResultsProduced);

        val result = polling.waitFor();

        assertThat(result.getCompletionStatus()).isEqualTo(QueryStatus.CompletionStatus.RESULTS_PRODUCED);
        assertThat(result.getChunkCount()).isEqualTo(3);
        assertThat(result.allResultsProduced()).isTrue();
        verifyGetQueryInfo(1);
    }

    @Test
    public void whenOptionalQueryInfoReceived_shouldSkipAndContinuePolling() throws Exception {
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
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
        val polling = DataCloudQueryPolling.of(
                getQueryClientWithTimeout(TEST_TIMEOUT), true, QueryStatus::allResultsProduced);

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
        val polling = DataCloudQueryPolling.of(getQueryClientWithTimeout(SHORT_TIMEOUT), true, status -> false);

        assertThatThrownBy(polling::waitFor).isInstanceOf(SQLException.class).satisfies(ex -> {
            String msg = ex.getMessage();
            boolean ok = msg.contains("Predicate was not satisfied before timeout.")
                    || msg.contains("Failed to get query status response.");
            assertThat(ok).as("message contains timeout or failed-to-get").isTrue();
        });

        verifyGetQueryInfoAtLeast(2);
    }
}
