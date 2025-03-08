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

import static com.salesforce.datacloud.jdbc.util.ThrowingSupplier.rethrowLongSupplier;
import static com.salesforce.datacloud.jdbc.util.ThrowingSupplier.rethrowSupplier;

import com.salesforce.datacloud.jdbc.core.DataCloudQueryStatus;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AdaptiveQueryStatusListener implements QueryStatusListener {
    @Getter
    private final String queryId;

    @Getter
    private final String query;

    private final HyperGrpcClientExecutor client;

    private final Duration timeout;

    private final Iterator<ExecuteQueryResponse> response;

    private final AdaptiveQueryStatusPoller headPoller;

    public static AdaptiveQueryStatusListener of(String query, HyperGrpcClientExecutor client, Duration timeout)
            throws SQLException {
        try {
            val response = client.executeAdaptiveQuery(query);
            val queryId = response.next().getQueryInfo().getQueryStatus().getQueryId();

            return new AdaptiveQueryStatusListener(
                    queryId, query, client, timeout, response, new AdaptiveQueryStatusPoller(queryId, client));
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createQueryException(query, ex);
        }
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public String getStatus() {
        return client.getQueryStatus(queryId)
                .map(DataCloudQueryStatus::getCompletionStatus)
                .map(Enum::name)
                .findFirst()
                .orElse("UNKNOWN");
    }

    @Override
    public DataCloudResultSet generateResultSet() {
        return StreamingResultSet.of(query, this);
    }

    @Override
    public Stream<QueryResult> stream() throws SQLException {
        return Stream.<Supplier<Stream<QueryResult>>>of(this::head, rethrowSupplier(this::tail))
                .flatMap(Supplier::get);
    }

    private Stream<QueryResult> head() {
        return StreamUtilities.toStream(response)
                .map(headPoller::map)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private Stream<QueryResult> tail() throws SQLException {
        return StreamUtilities.lazyLimitedStream(this::infiniteChunks, rethrowLongSupplier(this::getChunkLimit))
                .flatMap(UnaryOperator.identity());
    }

    private Stream<Stream<QueryResult>> infiniteChunks() {
        return LongStream.iterate(1, n -> n + 1).mapToObj(this::tryGetQueryResult);
    }

    private long getChunkLimit() throws SQLException {
        if (headPoller.pollChunkCount() > 1) {
            val status = blockUntilReady(timeout);
            return status.getChunkCount() - 1;
        }

        return 0;
    }

    private Stream<QueryResult> tryGetQueryResult(long chunkId) {
        return StreamUtilities.tryTimes(
                        3,
                        () -> client.getQueryResult(queryId, chunkId, true),
                        throwable -> log.warn(
                                "Error when getting chunk for query. queryId={}, chunkId={}",
                                queryId,
                                chunkId,
                                throwable))
                .map(StreamUtilities::toStream)
                .orElse(Stream.empty());
    }

    private DataCloudQueryStatus blockUntilReady(Duration timeout) throws DataCloudJDBCException {
        val deadline = Instant.now().plus(timeout);
        val last = new AtomicReference<DataCloudQueryStatus>();

        while (Instant.now().isBefore(deadline)) {
            val isReady = client.getQueryStatus(queryId)
                    .peek(last::set)
                    .anyMatch(t -> t.isResultProduced() || t.isExecutionFinished());
            if (isReady) {
                return last.get();
            }
        }

        throw new DataCloudJDBCException(BEFORE_READY + ". queryId=" + queryId + ", timeout=" + timeout);
    }
}
