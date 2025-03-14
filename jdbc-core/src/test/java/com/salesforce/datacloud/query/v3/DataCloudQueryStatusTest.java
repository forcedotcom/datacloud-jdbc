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
package com.salesforce.datacloud.query.v3;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.UUID;
import java.util.function.Consumer;
import lombok.val;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryStatus;

class DataCloudQueryStatusTest {
    private static QueryInfo random(Consumer<QueryStatus.Builder> update) {
        val queryStatus = QueryStatus.newBuilder()
                .setChunkCount(1)
                .setRowCount(100)
                .setProgress(0.5)
                .setCompletionStatus(QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED);
        update.accept(queryStatus);
        return QueryInfo.newBuilder().setQueryStatus(queryStatus).build();
    }

    @Test
    void testRunningOrUnspecified() {
        val actual = DataCloudQueryStatus.of(random(s -> {}));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isFalse();
            assertThat(t.isExecutionFinished()).isFalse();
            assertThat(t.isResultProduced()).isFalse();
        });
    }

    @Test
    void testExecutionFinished() {
        val actual = DataCloudQueryStatus.of(random(s -> s.setCompletionStatus(QueryStatus.CompletionStatus.FINISHED)));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isTrue();
            assertThat(t.isExecutionFinished()).isTrue();
            assertThat(t.isResultProduced()).isFalse();
        });
    }

    @Test
    void testResultsProduced() {
        val actual = DataCloudQueryStatus.of(
                random(s -> s.setCompletionStatus(QueryStatus.CompletionStatus.RESULTS_PRODUCED)));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isTrue();
            assertThat(t.isExecutionFinished()).isFalse();
            assertThat(t.isResultProduced()).isTrue();
        });
    }

    @Test
    void testQueryId() {
        val queryId = UUID.randomUUID().toString();
        val queryInfo = random(s -> s.setQueryId(queryId));
        val actual = DataCloudQueryStatus.of(queryInfo);

        assertThat(actual)
                .isPresent()
                .map(DataCloudQueryStatus::getQueryId)
                .get()
                .isEqualTo(queryId);
    }

    @Test
    void testProgress() {
        val progress = 0.35;
        val queryInfo = random(s -> s.setProgress(progress));
        val actual = DataCloudQueryStatus.of(queryInfo);

        assertThat(actual)
                .isPresent()
                .map(DataCloudQueryStatus::getProgress)
                .get()
                .isEqualTo(progress);
    }

    @Test
    void testChunkCount() {
        val chunks = 5678L;
        val queryInfo = random(s -> s.setChunkCount(chunks));
        val actual = DataCloudQueryStatus.of(queryInfo);

        assertThat(actual)
                .isPresent()
                .map(DataCloudQueryStatus::getChunkCount)
                .get()
                .isEqualTo(chunks);
    }

    @Test
    void testRowCount() {
        val rows = 1234L;
        val queryInfo = random(s -> s.setRowCount(rows));
        val actual = DataCloudQueryStatus.of(queryInfo);

        assertThat(actual)
                .isPresent()
                .map(DataCloudQueryStatus::getRowCount)
                .get()
                .isEqualTo(rows);
    }
}
