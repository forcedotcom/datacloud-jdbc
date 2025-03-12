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

import com.salesforce.datacloud.jdbc.core.client.DataCloudQueryStatus;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Unstable;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;

@Unstable
@Slf4j
@UtilityClass
public class DataCloudQueryPolling {
    public static DataCloudQueryStatus waitForRowsAvailable(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            long offset,
            long limit,
            Duration timeout,
            boolean allowLessThan)
            throws DataCloudJDBCException {
        return waitForQueryStatus(stub, queryId, timeout, status -> {
            if (allowLessThan) {
                return status.getRowCount() > offset;
            } else {
                return status.getRowCount() >= offset + limit;
            }
        });
    }

    public static DataCloudQueryStatus waitForResultsProduced(
            HyperServiceGrpc.HyperServiceBlockingStub stub, String queryId, Duration timeout)
            throws DataCloudJDBCException {
        return waitForQueryStatus(stub, queryId, timeout, DataCloudQueryStatus::allResultsProduced);
    }

    public static DataCloudQueryStatus waitForQueryStatus(
            HyperServiceGrpc.HyperServiceBlockingStub stub,
            String queryId,
            Duration timeout,
            Predicate<DataCloudQueryStatus> predicate)
            throws DataCloudJDBCException {
        val last = new AtomicReference<DataCloudQueryStatus>();
        val deadline = Instant.now().plus(timeout);
        val attempts = new AtomicInteger(1);

        val retry = new RetryPolicy<DataCloudQueryStatus>()
                .withMaxDuration(timeout)
                .handle(StatusRuntimeException.class)
                .handleIf(e -> {
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
            return Failsafe.with(retry)
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
        val param = QueryInfoParam.newBuilder().setQueryId(queryId).build();
        while (Instant.now().isBefore(deadline)) {
            val info = stub.getQueryInfo(param);
            while (info.hasNext()) {
                val next = DataCloudQueryStatus.of(info.next());
                next.ifPresent(last::set);
                if (predicate.test(last.get())) {
                    return last.get();
                }
            }
        }
        return last.get();
    }
}
