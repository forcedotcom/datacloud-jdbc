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

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.hyperdb.grpc.ExecuteQueryResponse;
import com.salesforce.hyperdb.grpc.QueryInfo;
import com.salesforce.hyperdb.grpc.QueryParam;
import com.salesforce.hyperdb.grpc.QueryResult;
import com.salesforce.hyperdb.grpc.QueryStatus;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;

@Builder
class Context {
    @Getter
    private final QueryParam.TransferMode transferMode;

    @Getter
    private final HyperGrpcClientExecutor client;

    @Builder.Default
    @Getter
    private final int rowsPerPage = -1;

    @Builder.Default
    @Getter
    private final AtomicLong index = new AtomicLong();

    @Getter
    private String queryId;

    @Getter
    private final AtomicBoolean omitSchema = new AtomicBoolean(false);

    @Getter
    @Setter
    private QueryStatus queryStatus;

    long getMax() {
        if (queryStatus != null) {
            return isRowBasedPagination() ? queryStatus.getRowCount() : queryStatus.getChunkCount();
        }

        return -1;
    }

    @Getter(lazy = true)
    private final boolean rowBasedPagination = this.rowsPerPage > 0;

    @SneakyThrows
    QueryResult peekQueryStatusAndExtractQueryResult(ExecuteQueryResponse response) {
        if (response == null) {
            return null;
        }

        if (this.queryId == null) {
            this.queryId = Optional.of(response)
                    .map(ExecuteQueryResponse::getQueryInfo)
                    .map(QueryInfo::getQueryStatus)
                    .map(QueryStatus::getQueryId)
                    .orElseThrow(() -> new DataCloudJDBCException("No QueryId was supplied by the server"));
        }

        Optional.of(response)
                .map(ExecuteQueryResponse::getQueryInfo)
                .map(QueryInfo::getQueryStatus)
                .ifPresent(this::setQueryStatus);

        if (response.hasQueryResult()) {
            return response.getQueryResult();
        }

        return null;
    }

    Stream<QueryInfo> streamQueryInfo() {
        val iterator = client.getQueryInfoStreaming(queryId);
        return StreamUtilities.toStream(iterator);
    }
}
