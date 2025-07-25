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
import com.salesforce.datacloud.query.v3.QueryStatus;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
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

    public static QueryStatus waitFor(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Deadline deadline,
            Predicate<QueryStatus> predicate)
            throws DataCloudJDBCException {
        val last = new AtomicReference<QueryStatus>();
        val retryAttempts = new AtomicInteger(0);

        // RetryPolicy fails if remainingDuration is zero or negative
        val remainingDuration = deadline.getRemaining();
        if (remainingDuration.isZero() || remainingDuration.isNegative()) {
            throw new DataCloudJDBCException(
                    "Query status polling timed out. queryId=" + queryId + ", lastStatus=" + last.get());
        }

        val retryPolicy = RetryPolicy.<QueryStatus>builder()
                .withMaxDuration(remainingDuration)
                .handleIf(e -> {
                    if (!(e instanceof StatusRuntimeException)) {
                        log.error("Got an unexpected exception when getting query status for queryId={}", queryId, e);
                        return false;
                    }

                    if (last.get() == null) {
                        log.error(
                                "Failed to get query status response, will not try again. queryId={}, retryAttempts={}",
                                queryId,
                                retryAttempts.get(),
                                e);
                        return false;
                    }

                    if (deadline.hasPassed()) {
                        log.error(
                                "Reached deadline for polling query status, will not try again. queryId={}, retryAttempts={}, lastStatus={}",
                                queryId,
                                retryAttempts.get(),
                                last.get(),
                                e);
                        return false;
                    }

                    log.warn(
                            "We think this error was a server timeout, will try again. queryId={}, retryAttempts={}, lastStatus={}",
                            queryId,
                            retryAttempts.get(),
                            last.get());
                    return true;
                })
                .build();

        try {
            AtomicInteger attempts = new AtomicInteger(0);
            QueryStatus result = null;

            do {
                if (attempts.getAndIncrement() > 0) {
                    log.info(
                            "The predicate was not satisfied after an iteration through the stream. queryId={}, attempts={}, lastStatus={}",
                            queryId,
                            attempts.get(),
                            result);
                }

                result = Failsafe.with(retryPolicy)
                        .get(() -> waitForWithoutRetry(stub, queryId, deadline, last, attempts, predicate));
            } while (!predicate.test(result));

            return result;
        } catch (FailsafeException ex) {
            throw new DataCloudJDBCException(
                    "Failed to get query status response. queryId=" + queryId + ", attempts=" + retryAttempts.get()
                            + ", lastStatus=" + last.get(),
                    ex.getCause());
        } catch (StatusRuntimeException ex) {
            throw new DataCloudJDBCException("Failed to get query status response. queryId=" + queryId, ex);
        }
    }

    static QueryStatus waitForWithoutRetry(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Deadline deadline,
            AtomicReference<QueryStatus> last,
            AtomicInteger times,
            Predicate<QueryStatus> predicate)
            throws DataCloudJDBCException {
        times.getAndIncrement();
        val param = QueryInfoParam.newBuilder()
                .setQueryId(queryId)
                .setStreaming(true)
                .build();
        val remaining = deadline.getRemaining();
        while (!remaining.isZero() && !remaining.isNegative()) {
            val info = stub.withDeadlineAfter(remaining.toMillis(), TimeUnit.MILLISECONDS)
                    .getQueryInfo(param);

            val mapped = StreamUtilities.toStream(info)
                    .map(QueryStatus::of)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .peek(last::set)
                    .iterator();

            while (mapped.hasNext()) {
                val status = mapped.next();
                if (predicate.test(status)) {
                    return status;
                }

                if (status.allResultsProduced()) {
                    throw new DataCloudJDBCException("query completed but predicate was not satisfied. queryId="
                            + queryId + ", finalStatus=" + status);
                }
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
