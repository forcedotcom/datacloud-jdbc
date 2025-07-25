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
import static org.junit.jupiter.api.Named.named;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Value;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import salesforce.cdp.hyperdb.v1.QueryInfo;

class QueryStatusTest {
    private static QueryInfo setup(Consumer<salesforce.cdp.hyperdb.v1.QueryStatus.Builder> update) {
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
        val actual = QueryStatus.of(setup(s -> {}));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isFalse();
            assertThat(t.isExecutionFinished()).isFalse();
            assertThat(t.isResultProduced()).isFalse();
        });
    }

    @Test
    void testExecutionFinished() {
        val actual = QueryStatus.of(
                setup(s -> s.setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.FINISHED)));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isTrue();
            assertThat(t.isExecutionFinished()).isTrue();
            assertThat(t.isResultProduced()).isFalse();
        });
    }

    @Test
    void testResultsProduced() {
        val actual = QueryStatus.of(setup(
                s -> s.setCompletionStatus(salesforce.cdp.hyperdb.v1.QueryStatus.CompletionStatus.RESULTS_PRODUCED)));

        assertThat(actual).isPresent().get().satisfies(t -> {
            assertThat(t.allResultsProduced()).isTrue();
            assertThat(t.isExecutionFinished()).isFalse();
            assertThat(t.isResultProduced()).isTrue();
        });
    }

    @Test
    void testQueryId() {
        val queryId = UUID.randomUUID().toString();
        val queryInfo = setup(s -> s.setQueryId(queryId));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getQueryId).get().isEqualTo(queryId);
    }

    @Test
    void testProgress() {
        val progress = 0.35;
        val queryInfo = setup(s -> s.setProgress(progress));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getProgress).get().isEqualTo(progress);
    }

    @Test
    void testChunkCount() {
        val chunks = 5678L;
        val queryInfo = setup(s -> s.setChunkCount(chunks));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getChunkCount).get().isEqualTo(chunks);
    }

    @Test
    void testRowCount() {
        val rows = 1234L;
        val queryInfo = setup(s -> s.setRowCount(rows));
        val actual = QueryStatus.of(queryInfo);

        assertThat(actual).isPresent().map(QueryStatus::getRowCount).get().isEqualTo(rows);
    }

    @Test
    @DisplayName("'first' style predicate returns true for any status")
    void shouldAlwaysReturnTrue() {
        final Predicate<QueryStatus> sut = s -> true;
        val status = mock(QueryStatus.class);

        Assertions.assertThat(sut.test(status)).isTrue();
        Assertions.assertThat(sut.test(null)).isTrue();
    }

    private static Stream<Arguments> cases() {
        return Stream.of(
                        setup("should return true when exact count is available", 10, 5, 15, true),
                        setup("should return true when more is available than needed", 10, 5, 20, true),
                        setup("should return false when insufficient count is available", 10, 5, 14, false),
                        setup("should return true when offset is zero and limit matches available", 0, 10, 10, true),
                        setup("should return false when offset is zero and limit exceeds available", 0, 10, 9, false),
                        setup("should handle zero offset and limit", 0, 0, 0, true),
                        setup("should handle single availability check", 0, 1, 1, true),
                        setup("should handle boundary condition at exact limit", 99, 1, 100, true))
                .map(Arguments::arguments);
    }

    @ParameterizedTest
    @MethodSource("cases")
    @DisplayName("QueryStatus.Predicates.rowsAvailable")
    void testRowsAvailable(Constraints args) {
        final Predicate<QueryStatus> sut = s -> s.allResultsProduced() || s.getRowCount() >= args.offset + args.limit;

        val status = mock(QueryStatus.class);
        when(status.getRowCount()).thenReturn(args.actual);

        Assertions.assertThat(sut.test(status)).isEqualTo(args.expecation);
    }

    @ParameterizedTest
    @MethodSource("cases")
    @DisplayName("QueryStatus.Predicates.chunksAvailable")
    void testChunksAvailable(Constraints args) {
        final Predicate<QueryStatus> sut = s -> s.allResultsProduced() || s.getChunkCount() >= args.offset + args.limit;

        val status = mock(QueryStatus.class);
        when(status.getChunkCount()).thenReturn(args.actual);

        Assertions.assertThat(sut.test(status)).isEqualTo(args.expecation);
    }

    @Value
    private static class Constraints {
        long offset;
        long limit;
        long actual;
        boolean expecation;
    }

    private static Named<Constraints> setup(String name, long offset, long limit, long actual, boolean expecation) {
        return named(name, new Constraints(offset, limit, actual, expecation));
    }
}
