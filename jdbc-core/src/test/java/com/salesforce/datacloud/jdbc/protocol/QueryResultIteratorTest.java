/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
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

public class QueryResultIteratorTest extends InterceptedHyperTestBase {

    private static final String TEST_QUERY = "SELECT * FROM test_table adaptiveTest";
    private static final String TEST_QUERY_ID = "test-query-123";

    private HyperServiceGrpc.HyperServiceBlockingStub setupStub() {
        return getInterceptedGrpcExecutor().getStub().withDeadlineAfter(30000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void whenExecuteQueryReturnsFinishedStatus_shouldNeverCallGetQueryInfo() {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1));
        val iterator = QueryResultIterator.of(stub, params);
        iterator.hasNext();

        assertThat(iterator.getQueryStatus().getQueryId()).isEqualTo(TEST_QUERY_ID);
        assertThat(iterator.hasNext()).isFalse();
        assertThat(QueryStatus.allResultsProduced(iterator.getQueryStatus())).isTrue();
        verifyGetQueryInfo(0);
        verifyGetQueryResult(0);
    }

    @ParameterizedTest
    @MethodSource("finishedStatusWithMultipleChunks")
    public void whenExecuteQueryReturnsFinishedStatusWithMultipleChunks_shouldCallGetQueryResultOnlyForAvailableChunks(
            int chunkCount) {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, chunkCount));

        for (int i = 1; i < chunkCount; i++) {
            setupFakeChunk(i);
        }

        val iterator = QueryResultIterator.of(stub, params);

        while (iterator.hasNext()) {
            iterator.next();
        }

        verifyGetQueryInfo(0);
        verifyGetQueryResult(chunkCount - 1);
    }

    @Test
    public void whenGetQueryInfoReturnsFinishedStatus_shouldNotCallGetQueryInfoAgain() {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = QueryResultIterator.of(stub, params);

        assertThat(iterator.hasNext()).isFalse();

        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenGetQueryInfoReturnsFinishedStatusWithAdditionalChunks_shouldCallGetQueryResultOnce() {
        val stub = setupStub();
        val params = setupExecuteQuery(
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
        val iterator = QueryResultIterator.of(stub, params);

        while (iterator.hasNext()) {
            iterator.next();
        }

        verifyGetQueryInfo(1);
        verifyGetQueryResult(2);
    }

    @Test
    public void whenQueryIsRunning_shouldCallGetQueryInfo() {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = QueryResultIterator.of(stub, params);

        assertThat(iterator.hasNext()).isFalse();

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenResultsProducedStatus_shouldNotCallGetQueryInfoAgain() {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED, 2));

        setupFakeChunk(1);
        val iterator = QueryResultIterator.of(stub, params);

        while (iterator.hasNext()) {
            iterator.next();
        }

        assertThat(QueryStatus.allResultsProduced(iterator.getQueryStatus())).isTrue();
        verifyGetQueryResult(1);
        verifyGetQueryInfo(0);
    }

    @ParameterizedTest
    @MethodSource("allCompletionStatuses")
    public void shouldReportCorrectQueryStatus(
            salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus inputStatus,
            salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus expectedStatus) {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, inputStatus, 1),
                executeQueryResponseWithData(Collections.emptyList()));
        val iterator = QueryResultIterator.of(stub, params);
        iterator.hasNext();

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
    public void whenExecuteQueryReturnsRunningWithNoAdditionalChunks_shouldCallGetQueryInfo() {
        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(
                        TEST_QUERY_ID,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        1));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = QueryResultIterator.of(stub, params);

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenRunningStatusWithMultipleChunks_shouldCallGetQueryResultThenGetQueryInfo() {
        val stub = setupStub();
        val params = setupExecuteQuery(
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
        val iterator = QueryResultIterator.of(stub, params);

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
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED),
                Arguments.of(
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED),
                Arguments.of(
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED,
                        salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED));
    }

    @Test
    public void whenOptionalExecuteQueryResponseReceived_shouldSkipAndContinueProcessing() {
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

        val stub = setupStub();
        val params = setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                responses.toArray(new ExecuteQueryResponse[0]));
        val iterator = QueryResultIterator.of(stub, params);

        assertThat(iterator.hasNext()).isFalse();
        assertThat(QueryStatus.allResultsProduced(iterator.getQueryStatus())).isTrue();
        verifyGetQueryInfo(0);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenOptionalQueryInfoReceived_shouldSkipAndContinuePolling() {
        val stub = setupStub();
        val params = setupExecuteQuery(
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
        val iterator = QueryResultIterator.of(stub, params);

        assertThat(iterator.hasNext()).isFalse();
        assertThat(QueryStatus.allResultsProduced(iterator.getQueryStatus())).isTrue();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenExecuteQueryStreamThrowsCancelled_shouldRetryWithGetQueryInfo() {
        val queryInfoResponse = executeQueryResponse(
                TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1);

        val stub = setupStub();
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req ->
                        req.getQuery().equals(TEST_QUERY) && req.getTransferMode() == QueryParam.TransferMode.ADAPTIVE)
                .willReturn(GrpcMock.stream(GrpcMock.response(queryInfoResponse))
                        .and(GrpcMock.statusException(Status.CANCELLED))));

        setupGetQueryInfo(TEST_QUERY_ID, salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED, 1);
        val iterator = QueryResultIterator.of(
                stub,
                QueryParam.newBuilder()
                        .setQuery(TEST_QUERY)
                        .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                        .build());

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenGetQueryInfoStreamThrowsCancelled_shouldRetryWithGetQueryInfo() {
        val stub = setupStub();
        val params = setupExecuteQuery(
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
        val iterator = QueryResultIterator.of(stub, params);

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(4);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenExecuteQueryThrowsCancelledWithoutQueryId_shouldFailQuery() {
        val stub = setupStub();
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getExecuteQueryMethod())
                .withRequest(req ->
                        req.getQuery().equals(TEST_QUERY) && req.getTransferMode() == QueryParam.TransferMode.ADAPTIVE)
                .willReturn(GrpcMock.statusException(Status.CANCELLED)));
        assertThatThrownBy(() -> QueryResultIterator.of(
                                stub,
                                QueryParam.newBuilder()
                                        .setQuery(TEST_QUERY)
                                        .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                                        .build())
                        .hasNext())
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("CANCELLED");

        verifyGetQueryInfo(0);
        verifyGetQueryResult(0);
    }
}
