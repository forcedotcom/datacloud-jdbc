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

import static com.salesforce.datacloud.jdbc.core.listener.QueryStatusListener.BEFORE_READY;

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
class AsyncQueryStatusPoller implements QueryStatusPoller {
    private final String queryId;
    private final HyperGrpcClientExecutor client;

    private final AtomicReference<QueryStatus> lastStatus = new AtomicReference<>();

    @SneakyThrows
    private Optional<QueryInfo> getQueryInfo() {
        try {
            return Optional.ofNullable(client.getQueryInfo(queryId)).map(Iterator::next);
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException("Failed when getting query status", ex);
        }
    }

    private Optional<QueryStatus> fetchQueryStatus() {

        val status = getQueryInfo().map(QueryInfo::getQueryStatus);
        if (status.isPresent()) {
            this.lastStatus.set(status.get());
        }
        return status;
    }

    @Override
    public QueryStatus pollQueryStatus() {
        val status = Optional.ofNullable(this.lastStatus.get());
        val finished = status.map(QueryStatus::getCompletionStatus)
                .map(t -> t == QueryStatus.CompletionStatus.FINISHED)
                .orElse(false);

        return finished ? status.get() : fetchQueryStatus().orElse(null);
    }

    @Override
    public long pollChunkCount() throws SQLException {
        if (!pollIsReady()) {
            throw new DataCloudJDBCException(BEFORE_READY);
        }
        return lastStatus.get().getChunkCount();
    }
}
