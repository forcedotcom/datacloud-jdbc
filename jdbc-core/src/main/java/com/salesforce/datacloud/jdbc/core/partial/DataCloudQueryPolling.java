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
package com.salesforce.datacloud.jdbc.core.partial;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.jdbc.util.Unstable;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;

@Unstable
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataCloudQueryPolling {

    enum State {
        RESET_QUERY_INFO_STREAM,
        PROCESS_QUERY_INFO_STREAM,
        COMPLETED
    }

    private final HyperServiceGrpc.HyperServiceBlockingStub stub;
    private final String queryId;
    private final Deadline deadline;
    private final Predicate<DataCloudQueryStatus> predicate;

    private State state = State.RESET_QUERY_INFO_STREAM;
    private DataCloudQueryStatus lastStatus = null;
    private Iterator<QueryInfo> queryInfos = null;
    private long timeInState = System.currentTimeMillis();

    /**
     * Creates a new instance for polling query status.
     *
     * @param stub The gRPC stub to use for querying status
     * @param queryId The identifier of the query to check
     * @param deadline The deadline for polling operations
     * @param predicate The condition to check against the query status
     * @return A new DataCloudQueryPolling instance
     */
    public static DataCloudQueryPolling of(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Deadline deadline,
            Predicate<DataCloudQueryStatus> predicate) {
        return new DataCloudQueryPolling(stub, queryId, deadline, predicate);
    }

    /**
     * Waits for the status of the specified query to satisfy the given predicate, polling until the predicate returns true or the timeout is reached.
     * The predicate determines what condition you are waiting for. For example, to wait until at least a certain number of rows are available, use:
     * <pre>
     *     status -> status.allResultsProduced() || status.getRowCount() >= targetRows
     * </pre>
     * Or, to wait for enough chunks:
     * <pre>
     *     status -> status.allResultsProduced() || status.getChunkCount() >= targetChunks
     * </pre>
     *
     * @return The first status that satisfies the predicate, or the last status received before timeout
     * @throws DataCloudJDBCException if the server reports all results produced but the predicate returns false, or if the timeout is exceeded
     */
    public DataCloudQueryStatus waitFor() throws DataCloudJDBCException {

        while (!deadline.hasPassed() && state != State.COMPLETED) {
            try {
                processCurrentState();
            } catch (DataCloudJDBCException ex) {
                log.error("Failed to process current state, state={}, queryId={}", state, queryId, ex);
                throw ex;
            }
        }

        val finalStatus = lastStatus.get();

        if (predicate.test(finalStatus)) {
            return finalStatus;
        } else if (deadline.hasPassed()) {
            throw new DataCloudJDBCException(
                "Query status polling timed out. queryId=" + queryId + ", lastStatus=" + finalStatus);
        } else {
            throw new DataCloudJDBCException("Predicate was not satisfied when execution finished. queryId=" + queryId
                    + ", lastStatus=" + finalStatus);
        }
    }

    private void processCurrentState() throws DataCloudJDBCException {
        switch (state) {
            case RESET_QUERY_INFO_STREAM:
                queryInfos = getQueryInfoIterator();
                transitionTo(State.PROCESS_QUERY_INFO_STREAM);
                break;

            case PROCESS_QUERY_INFO_STREAM:
                if (queryInfos == null) {
                    transitionTo(State.RESET_QUERY_INFO_STREAM);
                    break;
                }

                try {
                    if (queryInfos.hasNext()) {
                        val nextInfo = queryInfos.next();
                        val mapped = DataCloudQueryStatus.of(nextInfo);

                        if (!mapped.isPresent()) {
                            throw new DataCloudJDBCException(
                                    "Query info could not be mapped to DataCloudQueryStatus. queryId=" + queryId
                                            + ", queryInfo=" + nextInfo);
                        }

                        lastStatus = mapped.get();
                    } else {
                        transitionTo(State.RESET_QUERY_INFO_STREAM);
                    }

                    val statusOpt = safelyGetNext(queryInfos).flatMap(DataCloudQueryStatus::of);
                    if (statusOpt.isPresent()) {
                        val currentStatus = statusOpt.get();
                        lastStatus.set(currentStatus);

                        if (predicate.test(currentStatus)) {
                            transitionTo(State.PREDICATE_SATISFIED);
                        } else if (currentStatus.isExecutionFinished()) {
                            transitionTo(State.EXECUTION_FINISHED_WITH_UNSATISFIED_PREDICATE);
                        }
                    } else {
                        throw new DataCloudJDBCException("Query info stream ended unexpectedly. queryId=" + queryId);
                        transitionTo(State.RESET_QUERY_INFO_STREAM);
                    }
                } catch (StatusRuntimeException ex) {
                    handleStatusRuntimeException(ex);
                    queryInfos = null;
                    transitionTo(State.RESET_QUERY_INFO_STREAM);
                }
                break;



            case PREDICATE_SATISFIED:
            case EXECUTION_FINISHED_WITH_UNSATISFIED_PREDICATE:
            case TIMEOUT_EXCEEDED:
                transitionTo(State.COMPLETED);
                break;

            default:
                throw new DataCloudJDBCException("Cannot calculate transition from unknown state, state=" + state);
        }
    }

    private void transitionTo(State newState) {
        val elapsed = getTimeInStateAndReset();
        log.trace("state transition from={}, to={}, elapsed={}, queryId={}", state, newState, elapsed, queryId);
        state = newState;
    }

    private Duration getTimeInStateAndReset() {
        val result = Duration.ofMillis(System.currentTimeMillis() - timeInState);
        timeInState = System.currentTimeMillis();
        return result;
    }

    private void handleStatusRuntimeException(StatusRuntimeException ex) throws StatusRuntimeException {
        if (ex.getStatus().getCode() == Status.Code.CANCELLED) {
            log.warn("Caught retryable CANCELLED exception, queryId={}, status={}", queryId, lastStatus.get(), ex);
        } else {
            log.error("Caught non-retryable exception, queryId={}, status={}", queryId, lastStatus.get(), ex);
            throw ex;
        }
    }

    private Iterator<QueryInfo> getQueryInfoIterator() {
        val param = QueryInfoParam.newBuilder()
                .setQueryId(queryId)
                .setStreaming(true)
                .build();

        val remaining = deadline.getRemaining();
        return stub.withDeadlineAfter(remaining.toMillis(), TimeUnit.MILLISECONDS)
                .getQueryInfo(param);
    }

    private static <T> Optional<T> safelyGetNext(Iterator<T> iterator) {
        if (iterator != null && iterator.hasNext()) {
            return Optional.ofNullable(iterator.next());
        }
        return Optional.empty();
    }
}
