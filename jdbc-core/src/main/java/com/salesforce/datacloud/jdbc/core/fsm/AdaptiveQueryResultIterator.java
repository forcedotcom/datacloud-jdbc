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

import static com.salesforce.datacloud.jdbc.core.fsm.InitialQueryInfoUtility.getInitialQueryInfo;

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Slf4j
@RequiredArgsConstructor
public class AdaptiveQueryResultIterator implements QueryResultIterator {

    enum State {
        PROCESS_EXECUTE_QUERY_STREAM,
        CHECK_FOR_MORE_DATA,
        PROCESS_QUERY_RESULT_STREAM,
        PROCESS_QUERY_INFO_STREAM,
        COMPLETED
    }

    private final HyperGrpcClientExecutor client;
    private final Context context;
    private State state = State.PROCESS_EXECUTE_QUERY_STREAM;

    public static AdaptiveQueryResultIterator of(String sql, HyperGrpcClientExecutor client, QueryTimeout timeout)
            throws SQLException {
        val response = client.executeQuery(sql, timeout);
        val context = Context.of(sql, response, timeout.getLocalDeadline());

        return new AdaptiveQueryResultIterator(client, context);
    }

    public static AdaptiveQueryResultIterator of(
            String sql, HyperGrpcClientExecutor client, QueryTimeout timeout, long maxRows, long maxBytes)
            throws SQLException {
        val response = client.executeQuery(sql, timeout, maxRows, maxBytes);
        val context = Context.of(sql, response, timeout.getLocalDeadline());

        return new AdaptiveQueryResultIterator(client, context);
    }

    @Override
    public String getQueryId() {
        return context.getQueryId();
    }

    @Override
    public QueryStatus getQueryStatus() {
        return context.status.get();
    }

    @Override
    public boolean hasNext() {
        // NOTE: this context.hasQueryResult() is why there's no State for "CONSUME_QUERY_RESULT" and is implicit
        // based on the result of that method.
        while (!context.hasQueryResult() && state != State.COMPLETED) {
            try {
                processCurrentState();
            } catch (StatusRuntimeException ex) {
                log.error("Failed to process current state, state={}, queryId={}", state, getQueryId(), ex);
                throw ex;
            } catch (DataCloudJDBCException ex) {
                log.error("Failed to process current state, state={}, queryId={}", state, getQueryId(), ex);
                throw new RuntimeException("Query processing failed: " + ex.getMessage(), ex);
            }
        }
        return context.hasQueryResult();
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return context.popQueryResult();
    }

    private void processCurrentState() throws DataCloudJDBCException {
        if (context.deadline.hasPassed()) {
            throw new DataCloudJDBCException(String.format(
                    "deadline has passed, state=%s, queryId=%s, status=%s", state, getQueryId(), getQueryStatus()));
        }

        switch (state) {
            case PROCESS_EXECUTE_QUERY_STREAM:
                try {
                    if (context.executeQueryResponses != null && context.executeQueryResponses.hasNext()) {
                        val response = context.executeQueryResponses.next();
                        if (response.hasQueryInfo()) {
                            context.updateQueryContext(response.getQueryInfo());
                        } else if (response.hasQueryResult()) {
                            context.setQueryResult(response.getQueryResult());
                        } else {
                            if (!response.getOptional()) {
                                throw new DataCloudJDBCException(
                                        "Got unexpected non-optional message from executeQuery response stream, queryId="
                                                + context.getQueryId());
                            }
                        }

                    } else {
                        transitionTo(State.CHECK_FOR_MORE_DATA);
                    }
                } catch (StatusRuntimeException ex) {
                    potentiallyTransitionToCheckForMoreData(ex);
                }
                break;

            case CHECK_FOR_MORE_DATA:
                if (context.hasMoreChunks()) {
                    val chunkId = context.getNextChunkId();
                    if (chunkId != null) {
                        context.queryResults = client.getQueryResult(context.getQueryId(), chunkId, true);
                        transitionTo(State.PROCESS_QUERY_RESULT_STREAM);
                    } else {
                        throw new DataCloudJDBCException(
                                "Unexpected null chunk id from getNextChunkId when hasMoreChunks reported true");
                    }
                } else if (!context.allResultsProduced()) {
                    context.queryInfos = client.getQueryInfo(context.getQueryId());
                    transitionTo(State.PROCESS_QUERY_INFO_STREAM);
                } else {
                    transitionTo(State.COMPLETED);
                }
                break;

            case PROCESS_QUERY_RESULT_STREAM:
                val nextResult = safelyGetNext(context.queryResults);
                if (nextResult.isPresent()) {
                    context.setQueryResult(nextResult.get());
                } else {
                    transitionTo(State.CHECK_FOR_MORE_DATA);
                }
                break;

            case PROCESS_QUERY_INFO_STREAM:
                while (!context.deadline.hasPassed() && !context.hasMoreChunks()) {
                    try {
                        val nextInfo = safelyGetNext(context.queryInfos);
                        if (nextInfo.isPresent()) {
                            context.updateQueryContext(nextInfo.get());
                        } else {
                            transitionTo(State.CHECK_FOR_MORE_DATA);
                            return;
                        }
                    } catch (StatusRuntimeException ex) {
                        potentiallyTransitionToCheckForMoreData(ex);
                    }
                }
                transitionTo(State.CHECK_FOR_MORE_DATA);
                break;

            case COMPLETED:
                throw new NoSuchElementException("No valid transition after state=" + state);

            default:
                throw new IllegalArgumentException("Cannot calculate transition from unknown state, state=" + state);
        }
    }

    /**
     * For both executeQueryResponse and getQueryInfo response streams, if we get a CANCELLED message then it means
     * that hyper decided the status of the query hadn't updated in time so we need to retry getting query status.
     * If we get a CANCELLED exception then we will remove the (now broken) stream and transition to CHECK_FOR_MORE_DATA
     */
    private void potentiallyTransitionToCheckForMoreData(StatusRuntimeException ex) throws StatusRuntimeException {
        if (ex.getStatus().getCode() == Status.Code.CANCELLED) {
            log.warn(
                    "Caught retryable exception, state={}, queryId={}, status={}",
                    state,
                    getQueryId(),
                    context.status,
                    ex);
            context.queryInfos = null;
            transitionTo(State.CHECK_FOR_MORE_DATA);
        } else {
            log.error(
                    "Caught non-retryable exception, state={}, queryId={}, status={}",
                    state,
                    getQueryId(),
                    context.status,
                    ex);
            throw ex;
        }
    }

    private void transitionTo(State newState) {
        val elapsed = context.getTimeInStateAndReset();
        log.trace("state transition from={}, to={}, elapsed={}", state, newState, elapsed);
        state = newState;
    }

    private static <T> Optional<T> safelyGetNext(Iterator<T> iterator) {
        if (iterator != null && iterator.hasNext()) {
            return Optional.ofNullable(iterator.next());
        }
        return Optional.empty();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class Context {
        static Context of(String sql, Iterator<ExecuteQueryResponse> response, Deadline deadline)
                throws DataCloudJDBCException {
            val queryInfo = getInitialQueryInfo(sql, response);
            val context = new Context(deadline, response);
            context.updateQueryContext(queryInfo);
            return context;
        }

        QueryResult queryResult;
        final Deadline deadline;
        final AtomicReference<QueryStatus> status = new AtomicReference<>();
        final AtomicLong highWater = new AtomicLong(1);

        final Iterator<ExecuteQueryResponse> executeQueryResponses;
        Iterator<QueryInfo> queryInfos = null;
        Iterator<QueryResult> queryResults = null;
        long timeInState = System.currentTimeMillis();

        Duration getTimeInStateAndReset() {
            val result = Duration.ofMillis(System.currentTimeMillis() - timeInState);
            timeInState = System.currentTimeMillis();
            return result;
        }

        void updateQueryContext(QueryInfo info) {
            if (info.getOptional()) {
                return; // Per hyper_service.proto: clients must ignore optional messages which they do not know
            }

            QueryStatus.of(info).ifPresent(status::set);
        }

        String getQueryId() {
            val current = status.get();
            return current != null ? current.getQueryId() : null;
        }

        boolean allResultsProduced() {
            val current = status.get();
            return current != null && current.allResultsProduced();
        }

        boolean hasMoreChunks() {
            val current = status.get();
            return current != null && highWater.get() < current.getChunkCount();
        }

        Long getNextChunkId() {
            val currentStatus = status.get();
            if (currentStatus == null) return null;

            val nextChunk = highWater.get();
            val availableChunks = currentStatus.getChunkCount();

            if (nextChunk < availableChunks && highWater.compareAndSet(nextChunk, nextChunk + 1)) {
                log.trace(
                        "chunk available nextChunk={}, availableChunks={}, status={}",
                        nextChunk,
                        availableChunks,
                        currentStatus.getCompletionStatus());
                return nextChunk;
            }

            log.trace(
                    "chunk not available, returning null nextChunk={}, availableChunks={}, status={}",
                    nextChunk,
                    availableChunks,
                    currentStatus.getCompletionStatus());
            return null;
        }

        void setQueryResult(QueryResult result) {
            this.queryResult = result;
        }

        QueryResult popQueryResult() {
            val result = this.queryResult;
            this.queryResult = null;
            return result;
        }

        boolean hasQueryResult() {
            return this.queryResult != null;
        }
    }
}
