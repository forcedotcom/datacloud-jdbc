/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.query.v3;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.UUID;
import java.util.function.Consumer;
import lombok.val;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryInfo;

class QueryStatusTest {
    private static QueryInfo queryInfoWith(Consumer<salesforce.cdp.hyperdb.v1.QueryStatus.Builder> update) {
        val queryStatus = salesforce.cdp.hyperdb.v1.QueryStatus.newBuilder()
                .setChunkCount(1)
                .setRowCount(100)
                .setProgress(0.5)
                .setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);
        update.accept(queryStatus);
        return QueryInfo.newBuilder().setQueryStatus(queryStatus).build();
    }

    @Test
    void testRunningOrUnspecified() {
        val actual = QueryStatus.of(queryInfoWith(s -> {}));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isFalse();
            assertThat(t.isExecutionFinished()).isFalse();
        });
    }

    @Test
    void testExecutionFinished() {
        val actual = QueryStatus.of(queryInfoWith(
                s -> s.setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isTrue();
            assertThat(t.isExecutionFinished()).isTrue();
        });
    }

    @Test
    void testResultsProduced() {
        val actual = QueryStatus.of(queryInfoWith(
                s -> s.setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED)));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isTrue();
            assertThat(t.isExecutionFinished()).isFalse();
        });
    }

    @Test
    void testQueryId() {
        val queryId = UUID.randomUUID().toString();
        val queryInfo = queryInfoWith(s -> s.setQueryId(queryId));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getQueryId).get().isEqualTo(queryId);
    }

    @Test
    void testProgress() {
        val progress = 0.35;
        val queryInfo = queryInfoWith(s -> s.setProgress(progress));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getProgress).get().isEqualTo(progress);
    }

    @Test
    void testChunkCount() {
        val chunks = 5678L;
        val queryInfo = queryInfoWith(s -> s.setChunkCount(chunks));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getChunkCount).get().isEqualTo(chunks);
    }

    @Test
    void testRowCount() {
        val rows = 1234L;
        val queryInfo = queryInfoWith(s -> s.setRowCount(rows));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getRowCount).get().isEqualTo(rows);
    }
}
