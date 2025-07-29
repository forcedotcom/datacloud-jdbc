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
package com.salesforce.datacloud.jdbc.core.fsm;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.HyperGrpcTestBase;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import java.time.Duration;
import java.util.Collections;
import java.util.stream.Stream;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

public class AdaptiveQueryResultIteratorTest extends HyperGrpcTestBase {

    private static final String TEST_QUERY = "SELECT * FROM test_table";
    private static final String TEST_QUERY_ID = "test-query-123";
    private static final QueryTimeout TEST_TIMEOUT = QueryTimeout.of(Duration.ofSeconds(30), Duration.ofSeconds(5));

    @Test
    public void whenExecuteQueryReturnsFinishedStatus_shouldNeverCallGetQueryInfo() throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 1));

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

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
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, chunkCount));

        for (int i = 1; i < chunkCount; i++) {
            setupFakeChunk(i);
        }

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        while (iterator.hasNext()) {
            iterator.next();
        }

        verifyGetQueryInfo(0);
        verifyGetQueryResult(chunkCount - 1);
    }

    @Test
    public void whenGetQueryInfoReturnsFinishedStatus_shouldNotCallGetQueryInfoAgain() throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1));

        setupGetQueryInfo(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 1);

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();

        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenGetQueryInfoReturnsFinishedStatusWithAdditionalChunks_shouldCallGetQueryResultOnce()
            throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1));

        setupGetQueryInfo(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 3);
        setupFakeChunk(1);
        setupFakeChunk(2);

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        while (iterator.hasNext()) {
            iterator.next();
        }

        verifyGetQueryInfo(1);
        verifyGetQueryResult(2);
    }

    @Test
    public void whenQueryIsRunning_shouldCallGetQueryInfo() throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1));

        setupGetQueryInfo(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 1);

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();

        verifyGetQueryInfo(1);
    }

    @Test
    public void whenResultsProducedStatus_shouldNotCallGetQueryInfoAgain() throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.RESULTS_PRODUCED, 2));

        setupFakeChunk(1);

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

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
            QueryStatus.CompletionStatus inputStatus, DataCloudQueryStatus.CompletionStatus expectedStatus)
            throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, inputStatus, 1));

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        if (inputStatus == QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED) {
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
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1));

        setupGetQueryInfo(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 1);

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

        assertThat(iterator.hasNext()).isFalse();
        verifyGetQueryInfo(1);
        verifyGetQueryResult(0);
    }

    @Test
    public void whenRunningStatusWithMultipleChunks_shouldCallGetQueryResultThenGetQueryInfo() throws Exception {
        setupExecuteQuery(
                TEST_QUERY_ID,
                TEST_QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(TEST_QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 3));

        for (int i = 1; i < 3; i++) {
            setupFakeChunk(i);
        }
        setupGetQueryInfo(TEST_QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 4);
        setupFakeChunk(3);

        val iterator = AdaptiveQueryResultIterator.of(TEST_QUERY, hyperGrpcClient, TEST_TIMEOUT);

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
                        QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED,
                        DataCloudQueryStatus.CompletionStatus.RUNNING),
                Arguments.of(
                        QueryStatus.CompletionStatus.RESULTS_PRODUCED,
                        DataCloudQueryStatus.CompletionStatus.RESULTS_PRODUCED),
                Arguments.of(QueryStatus.CompletionStatus.FINISHED, DataCloudQueryStatus.CompletionStatus.FINISHED));
    }
}
