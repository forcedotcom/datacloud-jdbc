/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.fsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.Status;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;

public class AdaptiveQueryResultIteratorTest extends InterceptedHyperTestBase {

    private static final String TEST_QUERY = "SELECT * FROM test_table";
    private static final String TEST_QUERY_ID = "test-query-123";
    private static final QueryTimeout TEST_TIMEOUT = QueryTimeout.of(Duration.ofSeconds(30), Duration.ofSeconds(5));

    @Test
    public void whenExecuteQueryReturnsFinishedStatus_shouldNeverCallGetQueryInfo() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1));
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.getQueryId()).isEqualTo(TEST_QUERY_ID);
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.getQueryStatus().allResultsProduced()).isTrue();
        verifyGetQueryInfo(0);
        verifyGetQueryResult(0);
    }

    @ParameterizedTest
    @MethodSource("finishedStatusWithMultipleChunks")
    public void whenExecuteQueryReturnsFinishedStatusWithMultipleChunks_shouldCallGetQueryResultOnlyForAvailableChunks(
            int chunkCount) throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, chunkCount));

        for (int i = 1; i < chunkCount; i++) {
            setupFakeChunk(i);
        }

        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        while (iterator.hasNext()) {
            iterator.next();
        }

        verifyGetQueryInfo(0);
        verifyGetQueryResult(chunkCount - 1);
    }

    @Test
    public void whenGetQueryInfoReturnsFinishedStatus_shouldNotCallGetQueryInfoAgain() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();

        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenGetQueryInfoReturnsFinishedStatusWithAdditionalChunks_shouldCallGetQueryResultOnce()
            throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 3);
        setupFakeChunk(1);
        setupFakeChunk(2);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        while (iterator.hasNext()) {
            iterator.next();
        }

        verifyGetQueryInfo(1);
        verifyGetQueryResult(2);
    }

    @Test
    public void whenQueryIsRunning_shouldCallGetQueryInfo() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenResultsProducedStatus_shouldNotCallGetQueryInfoAgain() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED, 2));

        setupFakeChunk(1);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        while (iterator.hasNext()) {
            iterator.next();
        }

        assertThat(iterator.getQueryStatus().allResultsProduced()).isTrue();
        verifyGetQueryResult(1);
        verifyGetQueryInfo(0);
    }

    @ParameterizedTest
    @MethodSource("allCompletionStatuses")
    public void shouldReportCorrectQueryStatus(
            salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus inputStatus,
            QueryStatus.CompletionStatus expectedStatus)
            throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, inputStatus, 1));
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        if (inputStatus == salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED) {
            assertThat(iterator.getQueryStatus().getCompletionStatus()).isEqualTo(expectedStatus);
        } else {
            iterator.hasNext();
            assertThat(iterator.getQueryStatus().getCompletionStatus()).isEqualTo(expectedStatus);
        }
    }

    private static Stream<Arguments> finishedStatusWithMultipleChunks() {
        return Stream.of(Arguments.of(2), Arguments.of(3), Arguments.of(5));
    }

    @Test
    public void whenExecuteQueryReturnsRunningWithNoAdditionalChunks_shouldCallGetQueryInfo() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenRunningStatusWithMultipleChunks_shouldCallGetQueryResultThenGetQueryInfo() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        3));

        for (int i = 1; i < 3; i++) {
            setupFakeChunk(i);
        }
        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 4);
        setupFakeChunk(3);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        iterator.next();
        iterator.next();

        verifyGetQueryInfo(0);
        verifyGetQueryResult(2);

        assertThat(iterator.hasNext()).isTrue();

        verifyGetQueryInfo(1);

        iterator.next();

        verifyGetQueryInfo(1);
        verifyGetQueryResult(3);

        assertThat(iterator.hasNext()).isFalse();
    }

    private void setupFakeChunk(int chunkNumber) {
        setupGetQueryResult(TEST_QUERY_ID, chunkNumber, 1, Collections.emptyList());
    }

    private static Stream<Arguments> allCompletionStatuses() {
        return Stream.of(
                Arguments.of(
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        QueryStatus.CompletionStatus.RUNNING),
                Arguments.of(
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED,
                        QueryStatus.CompletionStatus.RESULTS_PRODUCED),
                Arguments.of(
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED,
                        QueryStatus.CompletionStatus.FINISHED));
    }

    @Test
    public void whenOptionalExecuteQueryResponseReceived_shouldSkipAndContinueProcessing() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        val responses = Arrays.asList(
                ExecuteQueryResponse.newBuilder()
                        .setQueryInfo(QueryInfo.newBuilder()
                                .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                        .setQueryId(TEST_QUERY_ID)
                                        .setCompletionStatus(
                                                salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus
                                                        .RUNNING_OR_UNSPECIFIED)
                                        .setChunkCount(1)
                                        .build())
                                .build())
                        .build(),
                ExecuteQueryResponse.newBuilder().setOptional(true).build(),
                ExecuteQueryResponse.newBuilder()
                        .setQueryInfo(QueryInfo.newBuilder()
                                .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                        .setQueryId(TEST_QUERY_ID)
                                        .setCompletionStatus(
                                                salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                                        .setChunkCount(1)
                                        .build())
                                .build())
                        .build());

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                responses.toArray(new ExecuteQueryResponse[0]));
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.getQueryStatus().allResultsProduced()).isTrue();
        verifyGetQueryInfo(0);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenOptionalQueryInfoReceived_shouldSkipAndContinuePolling() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willProxyTo((request, observer) -> {
                    observer.onNext(QueryInfo.newBuilder().setOptional(true).build());

                    observer.onNext(QueryInfo.newBuilder()
                            .setQueryStatus(salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                                    .setQueryId(TEST_QUERY_ID)
                                    .setCompletionStatus(
                                            salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                                    .setChunkCount(1)
                                    .build())
                            .build());
                    observer.onCompleted();
                }));
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.getQueryStatus().allResultsProduced()).isTrue();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenExecuteQueryStreamThrowsCancelled_shouldRetryWithGetQueryInfo() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        val queryInfoResponse = executeQueryResponse(
                TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1);

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req ->
                        req.getQuery().equals(TEST_QUERY) && req.getTransferMode() == QueryParam.TransferMode.ADAPTIVE)
                .willReturn(GrpcMock.stream(GrpcMock.response(queryInfoResponse))
                        .and(GrpcMock.statusException(Status.CANCELLED))));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenGetQueryInfoStreamThrowsCancelled_shouldRetryWithGetQueryInfo() throws Exception {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        val status = salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                .setQueryId(TEST_QUERY_ID)
                .setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED)
                .setChunkCount(1)
                .build();

        val running = QueryInfo.newBuilder().setQueryStatus(status).build();
        val finished = QueryInfo.newBuilder()
                .setQueryStatus(status.toBuilder()
                        .setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)
                        .build())
                .build();

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .withRequest(req -> req.getQueryId().equals(TEST_QUERY_ID))
                .willReturn(GrpcMock.statusException(Status.CANCELLED))
                .nextWillReturn(running)
                .nextWillReturn(GrpcMock.statusException(Status.CANCELLED))
                .nextWillReturn(finished));
        val iterator = AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(4);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenExecuteQueryThrowsCancelledWithoutQueryId_shouldFailQuery() {
        val hyperGrpcClient = getInterceptedGrpcExecutor();

        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req ->
                        req.getQuery().equals(TEST_QUERY) && req.getTransferMode() == QueryParam.TransferMode.ADAPTIVE)
                .willReturn(GrpcMock.statusException(Status.CANCELLED)));
        assertThatThrownBy(() -> AdaptiveQueryResultIterator.of(true, TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Failed to execute query")
                .hasMessageContaining("QUERY: " + TEST_QUERY);

        verifyGetQueryInfo(0);
        verifyGetQueryResult(0);
    }
}
