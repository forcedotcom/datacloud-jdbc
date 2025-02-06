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

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class AsyncQueryStatusListener implements QueryStatusListener {
    @Getter
    private final String queryId;

    @Getter
    private final String query;

    private final HyperGrpcClientExecutor client;

    @Getter(value = AccessLevel.PRIVATE, lazy = true)
    private final AsyncQueryStatusPoller poller = new AsyncQueryStatusPoller(queryId, client);

    public static AsyncQueryStatusListener of(String query, HyperGrpcClientExecutor client) throws SQLException {
        try {
            val result = client.executeAsyncQuery(query).next();
            val id = result.getQueryInfo().getQueryStatus().getQueryId();

            return AsyncQueryStatusListener.builder()
                    .queryId(id)
                    .query(query)
                    .client(client)
                    .build();
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createQueryException(query, ex);
        }
    }

    @Override
    public boolean isReady() {
        return getPoller().pollIsReady();
    }

    @Override
    public String getStatus() {
        return Optional.of(getPoller())
                .map(AsyncQueryStatusPoller::pollQueryStatus)
                .map(QueryStatus::getCompletionStatus)
                .map(Enum::name)
                .orElse(null);
    }

    @Override
    public DataCloudResultSet generateResultSet() {
        return StreamingResultSet.of(query, this);
    }

    @Override
    public Stream<QueryResult> stream() throws SQLException {
        return StreamUtilities.lazyLimitedStream(this::infiniteChunks, this::getChunkLimit)
                .flatMap(UnaryOperator.identity());
    }

    private Stream<Stream<QueryResult>> infiniteChunks() {
        return LongStream.iterate(0, n -> n + 1).mapToObj(this::tryGetQueryResult);
    }

    @SneakyThrows
    private long getChunkLimit() {
        if (!isReady()) {
            throw new DataCloudJDBCException(BEFORE_READY);
        }

        return getPoller().pollChunkCount();
    }

    private Stream<QueryResult> tryGetQueryResult(long chunkId) {
        return StreamUtilities.tryTimes(
                        3,
                        () -> client.getQueryResult(queryId, chunkId, chunkId > 0),
                        throwable -> log.warn(
                                "Error when getting chunk for query. queryId={}, chunkId={}",
                                queryId,
                                chunkId,
                                throwable))
                .map(StreamUtilities::toStream)
                .orElse(Stream.empty());
    }
}
