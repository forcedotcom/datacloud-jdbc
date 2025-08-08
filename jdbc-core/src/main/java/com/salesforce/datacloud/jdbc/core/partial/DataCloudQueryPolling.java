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

import static com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler.createException;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.jdbc.util.Unstable;
import com.salesforce.datacloud.query.v3.QueryStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
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
    private final Predicate<QueryStatus> predicate;

    private State state = State.RESET_QUERY_INFO_STREAM;
    private QueryStatus lastStatus = null;
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
            Predicate<QueryStatus> predicate) {
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
    public QueryStatus waitFor() throws DataCloudJDBCException {
        Exception lastException = null;
        while (!deadline.hasPassed() && state != State.COMPLETED) {
            try {
                processCurrentState();
            } catch (StatusRuntimeException ex) {
                log.error("Caught unexpected exception from server. {}", this, ex);
                if (ex.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                    throw new DataCloudJDBCException("Predicate was not satisfied before timeout. " + this, ex);
                } else {
                    throw createException("Failed when getting query status. " + this, ex);
                }

            } catch (DataCloudJDBCException ex) {
                log.error("Failed to process current state. {}", this, ex);
                throw ex;
            }
        }

        if (lastStatus == null) {
            throw new DataCloudJDBCException("Failed to get query status response. " + this, lastException);
        } else if (predicate.test(lastStatus)) {
            return lastStatus;
        } else if (deadline.hasPassed()) {
            throw new DataCloudJDBCException("Predicate was not satisfied before timeout. " + this, lastException);
        } else {
            throw new DataCloudJDBCException(
                    "Predicate was not satisfied when execution finished. " + this, lastException);
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
                    if (!queryInfos.hasNext()) {
                        transitionTo(State.RESET_QUERY_INFO_STREAM);
                        break;
                    }

                    val info = queryInfos.next();
                    if (info.getOptional()) {
                        log.trace("Unexpected optional message, message={} {}", info, this);
                        break; // Per hyper_service.proto: clients must ignore optional messages which they do not know
                    }

                    val mapped = QueryStatus.of(info);

                    if (!mapped.isPresent()) {
                        throw new DataCloudJDBCException(
                                "Query info could not be mapped to DataCloudQueryStatus. queryId=" + queryId
                                        + ", queryInfo=" + info + ". " + this);
                    }

                    lastStatus = mapped.get();

                    if (predicate.test(lastStatus)) {
                        transitionTo(State.COMPLETED);
                    } else if (lastStatus.isExecutionFinished()) {
                        transitionTo(State.COMPLETED);
                    }

                } catch (StatusRuntimeException ex) {
                    handleStatusRuntimeException(ex);
                    queryInfos = null;
                    transitionTo(State.RESET_QUERY_INFO_STREAM);
                }
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
            log.warn("Caught retryable CANCELLED exception, {}", this, ex);
        } else {
            log.error("Caught non-retryable exception, {}", this, ex);
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

    @Override
    public String toString() {
        return String.format(
                "queryId=%s, state=%s, deadline=%s, status=%s", queryId, state.name(), deadline, lastStatus);
    }
}
