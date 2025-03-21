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
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.jdbc.util.Unstable;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

@Unstable
@Slf4j
@UtilityClass
public class DataCloudQueryPolling {
    public static DataCloudQueryStatus waitForRowsAvailable(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            long offset,
            long limit,
            Duration timeoutDuration,
            boolean allowLessThan)
            throws DataCloudJDBCException {

        Predicate<DataCloudQueryStatus> predicate = status -> {
            if (allowLessThan) {
                return status.getRowCount() > offset;
            } else {
                return status.getRowCount() >= offset + limit;
            }
        };

        val result = waitForQueryStatus(stub, queryId, timeoutDuration, predicate);

        if (predicate.test(result)) {
            return result;
        } else {
            if (allowLessThan) {
                throw new DataCloudJDBCException(
                        "Timed out waiting for new rows to be available. queryId=" + queryId + ", status=" + result);
            } else {
                throw new DataCloudJDBCException(
                        "Timed out waiting for enough rows to be available. queryId=" + queryId + ", status=" + result);
            }
        }
    }

    public static DataCloudQueryStatus waitForResultsProduced(
            HyperServiceGrpc.HyperServiceBlockingStub stub, String queryId, Duration timeout)
            throws DataCloudJDBCException {
        return waitForQueryStatus(stub, queryId, timeout, DataCloudQueryStatus::allResultsProduced);
    }

    public static DataCloudQueryStatus waitForQueryStatus(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Duration timeoutDuration,
            Predicate<DataCloudQueryStatus> predicate)
            throws DataCloudJDBCException {
        val last = new AtomicReference<DataCloudQueryStatus>();
        val deadline = Instant.now().plus(timeoutDuration);
        val attempts = new AtomicInteger(0);

        val retryPolicy = new RetryPolicy<DataCloudQueryStatus>()
                .withMaxDuration(timeoutDuration)
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

                    if (Instant.now().isAfter(deadline)) {
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
                });

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
            Instant deadline,
            AtomicReference<DataCloudQueryStatus> last,
            AtomicInteger times,
            Predicate<DataCloudQueryStatus> predicate) {
        times.getAndIncrement();
        val param = QueryInfoParam.newBuilder().setQueryId(queryId).setStreaming(true).build();
        while (Instant.now().isBefore(deadline)) {
            val info = stub.getQueryInfo(param);
            val matched = StreamUtilities.toStream(info)
                    .map(DataCloudQueryStatus::of)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .peek(last::set)
                    .filter(predicate::test)
                    .findFirst();

            if (matched.isPresent()) {
                return matched.get();
            }

            log.info("end of info stream, starting a new one if the timeout allows. last={}, remaining={}", last.get(), remaining(deadline));
        }

        log.warn("exceeded deadline getting query info. last={}", last.get());
        return last.get();
    }

    private Duration remaining(Instant deadline) {
        return Duration.between(Instant.now(), deadline);
    }
}
