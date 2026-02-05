/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

class AsyncQueryResultIteratorTest extends InterceptedHyperTestBase {

    private static final String TEST_QUERY = "SELECT * FROM test_table asyncTest";
    private static final String TEST_QUERY_ID = "async-test-query-123";

    private HyperServiceGrpc.HyperServiceStub setupStub() {
        return getInterceptedStub().withDeadlineAfter(30000, TimeUnit.MILLISECONDS);
    }

    /**
     * Tests that the async iterator correctly handles a scenario where:
     * 1. Initial executeQuery stream returns data quickly
     * 2. Query info polling hangs (simulating slow server response)
     * 3. Eventually completes when the polling returns
     *
     * This ensures the async nature allows other work to proceed while waiting.
     */
    @Test
    void whenQueryProducesDataThenHangsAtEnd_shouldCompleteSuccessfully() throws Exception {
        val stub = setupStub();

        val delayedInfoLatch = new CountDownLatch(1);
        val iteratorReceivedFirstResult = new CountDownLatch(1);

        // Setup executeQuery to return initial data with RUNNING status
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req -> req.getQuery().equals(TEST_QUERY))
                .willProxyTo((request, observer) -> {
                    // First response: query info with running status
                    observer.onNext(ExecuteQueryResponse.newBuilder()
                            .setQueryInfo(QueryInfo.newBuilder()
                                    .setQueryStatus(QueryStatus.newBuilder()
                                            .setQueryId(TEST_QUERY_ID)
                                            .setCompletionStatus(QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED)
                                            .setChunkCount(1)
                                            .build())
                                    .build())
                            .build());

                    // Second response: inline result data (fast)
                    observer.onNext(ExecuteQueryResponse.newBuilder()
                            .setQueryResult(QueryResult.newBuilder().build())
                            .build());

                    observer.onCompleted();
                }));

        // Setup getQueryInfo to delay before returning finished status
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    // Wait for the iterator to receive the first result before continuing
                    try {
                        iteratorReceivedFirstResult.await(5, TimeUnit.SECONDS);
                        // Simulate a delay in the query info response
                        delayedInfoLatch.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    // Return finished status
                    observer.onNext(QueryInfo.newBuilder()
                            .setQueryStatus(QueryStatus.newBuilder()
                                    .setQueryId(TEST_QUERY_ID)
                                    .setCompletionStatus(QueryStatus.CompletionStatus.FINISHED)
                                    .setChunkCount(1)
                                    .build())
                            .build());
                    observer.onCompleted();
                }));

        val queryParam = QueryParam.newBuilder()
                .setQuery(TEST_QUERY)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                .build();

        try (val iterator = AsyncQueryResultIterator.of(stub, queryParam)) {
            val resultCount = new AtomicInteger(0);

            // First call should return quickly with the inline result
            CompletableFuture<Optional<QueryResult>> firstFuture =
                    iterator.next().toCompletableFuture();

            // Should complete quickly since data is available
            Optional<QueryResult> firstResult = firstFuture.get(5, TimeUnit.SECONDS);
            assertThat(firstResult).isPresent();
            resultCount.incrementAndGet();

            // Signal that we received the first result
            iteratorReceivedFirstResult.countDown();

            // Second call will need to poll for query info - this will hang initially
            CompletableFuture<Optional<QueryResult>> secondFuture =
                    iterator.next().toCompletableFuture();

            // Verify the future is not completed yet (query info is delayed)
            assertThat(secondFuture.isDone()).isFalse();

            Thread.sleep(1000);

            // Release the delayed query info
            delayedInfoLatch.countDown();

            // Now it should complete with empty (no more results)
            Optional<QueryResult> secondResult = secondFuture.get(5, TimeUnit.SECONDS);
            assertThat(secondResult).isEmpty();

            // Verify final status
            assertThat(iterator.getQueryStatus().getCompletionStatus())
                    .isEqualTo(QueryStatus.CompletionStatus.FINISHED);
            assertThat(resultCount.get()).isEqualTo(1);
        }
    }

    @Test
    void whenExecuteQueryReturnsFinishedImmediately_shouldCompleteWithoutPolling() throws Exception {
        val stub = setupStub();

        // Setup executeQuery to return finished status immediately with inline data
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req -> req.getQuery().equals(TEST_QUERY))
                .willProxyTo((request, observer) -> {
                    observer.onNext(ExecuteQueryResponse.newBuilder()
                            .setQueryInfo(QueryInfo.newBuilder()
                                    .setQueryStatus(QueryStatus.newBuilder()
                                            .setQueryId(TEST_QUERY_ID)
                                            .setCompletionStatus(QueryStatus.CompletionStatus.FINISHED)
                                            .setChunkCount(1)
                                            .build())
                                    .build())
                            .build());

                    observer.onNext(ExecuteQueryResponse.newBuilder()
                            .setQueryResult(QueryResult.newBuilder().build())
                            .build());

                    observer.onCompleted();
                }));

        val queryParam = QueryParam.newBuilder()
                .setQuery(TEST_QUERY)
                .setOutputFormat(OutputFormat.ARROW_IPC)
                .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                .build();

        try (val iterator = AsyncQueryResultIterator.of(stub, queryParam)) {
            // First result should be available
            Optional<QueryResult> first = iterator.next().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(first).isPresent();

            // Second call should return empty (finished)
            Optional<QueryResult> second = iterator.next().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(second).isEmpty();

            assertThat(iterator.getQueryStatus().getCompletionStatus())
                    .isEqualTo(QueryStatus.CompletionStatus.FINISHED);
        }

        // Verify no query info polling was needed
        verifyGetQueryInfo(0);
    }
}
