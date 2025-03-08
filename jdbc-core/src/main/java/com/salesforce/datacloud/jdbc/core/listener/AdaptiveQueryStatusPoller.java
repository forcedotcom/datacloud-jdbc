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

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class AdaptiveQueryStatusPoller implements QueryStatusPoller {
    private final AtomicLong chunks = new AtomicLong(1);
    private final AtomicReference<QueryStatus> lastStatus = new AtomicReference<>();
    private final String queryId;
    private final HyperGrpcClientExecutor client;

    @SneakyThrows
    private Iterator<QueryInfo> getQueryInfoStreaming() {
        try {
            return client.getQueryInfo(queryId);
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException("Failed when getting query status", ex);
        }
    }

    public Optional<QueryResult> map(ExecuteQueryResponse item) {
        getQueryStatus(item).ifPresent(this::handleQueryStatus);
        return getQueryResult(item);
    }

    private void handleQueryStatus(QueryStatus status) {
        lastStatus.set(status);

        if (status.getChunkCount() > 1) {
            this.chunks.set(status.getChunkCount());
        }
    }

    private Optional<QueryStatus> getQueryStatus(ExecuteQueryResponse item) {
        if (item != null && item.hasQueryInfo()) {
            val info = item.getQueryInfo();
            if (info.hasQueryStatus()) {
                return Optional.of(info.getQueryStatus());
            }
        }

        return Optional.empty();
    }

    private Optional<QueryResult> getQueryResult(ExecuteQueryResponse item) {
        if (item != null && item.hasQueryResult()) {
            return Optional.of(item.getQueryResult());
        }

        return Optional.empty();
    }

    @Override
    public QueryStatus pollQueryStatus() {
        return lastStatus.get();
    }

    @Override
    public long pollChunkCount() {
        val status = Optional.ofNullable(this.lastStatus.get());
        val finalized = status.map(QueryStatus::getCompletionStatus)
                .map(t -> t == QueryStatus.CompletionStatus.FINISHED
                        || t == QueryStatus.CompletionStatus.RESULTS_PRODUCED)
                .orElse(false);

        if (finalized) {
            return chunks.get();
        }

        val queryInfos = getQueryInfoStreaming();
        val result = StreamUtilities.toStream(queryInfos)
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(QueryInfo::getQueryStatus)
                .filter(AdaptiveQueryStatusPoller::isChunksCompleted)
                .findFirst();

        result.ifPresent(it -> {
            val completion = it.getCompletionStatus();
            val chunkCount = it.getChunkCount();
            log.info("Polling chunk count. queryId={}, status={}, count={}", this.queryId, completion, chunkCount);
            chunks.set(chunkCount);
        });

        return chunks.get();
    }

    private static boolean isChunksCompleted(QueryStatus s) {
        val completion = s.getCompletionStatus();
        return completion == QueryStatus.CompletionStatus.RESULTS_PRODUCED
                || completion == QueryStatus.CompletionStatus.FINISHED;
    }
}
