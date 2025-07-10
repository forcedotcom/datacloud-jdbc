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
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.jdbc.util.Unstable;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;

@Unstable
@Slf4j
public final class DataCloudQueryPolling {
    private DataCloudQueryPolling() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DataCloudQueryStatus waitForChunksAvailable(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            long offset,
            long limit,
            Deadline deadline,
            boolean allowLessThan)
            throws DataCloudJDBCException {
        return waitForCountAvailabile(
                stub, queryId, offset, limit, deadline, allowLessThan, DataCloudQueryStatus::getChunkCount);
    }

    public static DataCloudQueryStatus waitForRowsAvailable(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            long offset,
            long limit,
            Deadline deadline,
            boolean allowLessThan)
            throws DataCloudJDBCException {
        return waitForCountAvailabile(
                stub, queryId, offset, limit, deadline, allowLessThan, DataCloudQueryStatus::getRowCount);
    }

    private static DataCloudQueryStatus waitForCountAvailabile(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            long offset,
            long limit,
            Deadline deadline,
            boolean allowLessThan,
            Function<DataCloudQueryStatus, Long> countSelector)
            throws DataCloudJDBCException {
        Predicate<DataCloudQueryStatus> predicate = status -> {
            val count = countSelector.apply(status);
            if (allowLessThan) {
                return count > offset;
            } else {
                return count >= offset + limit;
            }
        };

        val result = waitForQueryStatus(stub, queryId, deadline, predicate);

        if (predicate.test(result)) {
            return result;
        } else {
            if (allowLessThan) {
                throw new DataCloudJDBCException(
                        "Timed out waiting for new items to be available. queryId=" + queryId + ", status=" + result);
            } else {
                throw new DataCloudJDBCException("Timed out waiting for enough items to be available. queryId="
                        + queryId + ", status=" + result);
            }
        }
    }

    public static DataCloudQueryStatus waitForQueryStatus(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Deadline deadline,
            Predicate<DataCloudQueryStatus> predicate)
            throws DataCloudJDBCException {
        val last = new AtomicReference<DataCloudQueryStatus>();
        val attempts = new AtomicInteger(0);

        // RetryPolicy fails if remainingDuration is zero or negative
        val remainingDuration = deadline.getRemaining();
        if (remainingDuration.isZero() || remainingDuration.isNegative()) {
            throw new DataCloudJDBCException(
                    "Query status polling timed out. queryId=" + queryId + ", lastStatus=" + last.get());
        }

        val retryPolicy = RetryPolicy.<DataCloudQueryStatus>builder()
                .withMaxDuration(remainingDuration)
                .handleIf(e -> {
                    if (!(e instanceof StatusRuntimeException)) {
                        log.error("Got an unexpected exception when getting query status for queryId={}", queryId, e);
                        return false;
                    }

                    if (last.get() == null) {
                        log.error(
                                "Failed to get query status response, will not try again. queryId={}, attempts={}",
                                queryId,
                                attempts.get(),
                                e);
                        return false;
                    }

                    if (deadline.hasPassed()) {
                        log.error(
                                "Reached deadline for polling query status, will not try again. queryId={}, attempts={}, lastStatus={}",
                                queryId,
                                attempts.get(),
                                last.get(),
                                e);
                        return false;
                    }

                    log.warn(
                            "We think this error was a server timeout, will try again. queryId={}, attempts={}, lastStatus={}",
                            queryId,
                            attempts.get(),
                            last.get());
                    return true;
                })
                .build();

        try {
            return Failsafe.with(retryPolicy)
                    .get(() -> waitForQueryStatusWithoutRetry(stub, queryId, deadline, last, attempts, predicate));
        } catch (FailsafeException ex) {
            throw new DataCloudJDBCException(
                    "Failed to get query status response. queryId=" + queryId + ", attempts=" + attempts.get()
                            + ", lastStatus=" + last.get(),
                    ex.getCause());
        } catch (StatusRuntimeException ex) {
            throw new DataCloudJDBCException("Failed to get query status response. queryId=" + queryId, ex);
        }
    }

    @SneakyThrows
    static DataCloudQueryStatus waitForQueryStatusWithoutRetry(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Deadline deadline,
            AtomicReference<DataCloudQueryStatus> last,
            AtomicInteger times,
            Predicate<DataCloudQueryStatus> predicate) {
        times.getAndIncrement();
        val param = QueryInfoParam.newBuilder()
                .setQueryId(queryId)
                .setStreaming(true)
                .build();
        val remaining = deadline.getRemaining();
        while (!remaining.isZero() && !remaining.isNegative()) {
            val info = stub.withDeadlineAfter(remaining.toMillis(), TimeUnit.MILLISECONDS)
                    .getQueryInfo(param);
            val matched = StreamUtilities.toStream(info)
                    .map(DataCloudQueryStatus::of)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .peek(last::set)
                    .filter(predicate)
                    .findFirst();

            if (matched.isPresent()) {
                return matched.get();
            }

            if (Optional.ofNullable(last.get())
                    .map(DataCloudQueryStatus::allResultsProduced)
                    .orElse(false)) {
                log.warn("predicate did not match but all results were produced. last={}", last.get());
                return last.get();
            }

            log.info(
                    "end of info stream, starting a new one if the timeout allows. last={}, remaining={}",
                    last.get(),
                    deadline.getRemaining());
        }

        log.warn("exceeded deadline getting query info. last={}", last.get());
        return last.get();
    }
}
