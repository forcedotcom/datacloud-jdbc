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

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class SyncQueryStatusListener implements QueryStatusListener {
    @Getter
    private final String queryId;

    @Getter
    private final String query;

    private final AtomicReference<QueryStatus> status = new AtomicReference<>();

    private final Iterator<ExecuteQueryResponse> initial;

    public static SyncQueryStatusListener of(String query, HyperGrpcClientExecutor client) throws SQLException {
        val result = client.executeQuery(query);

        try {
            val id = getQueryId(result.next(), query);
            return SyncQueryStatusListener.builder()
                    .query(query)
                    .queryId(id)
                    .initial(result)
                    .build();
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createException("Failed to execute query: " + query, ex);
        }
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public String getStatus() {
        return Optional.ofNullable(status.get())
                .map(QueryStatus::getCompletionStatus)
                .map(Enum::name)
                .orElse(null);
    }

    @Override
    public DataCloudResultSet generateResultSet() {
        return StreamingResultSet.of(query, this);
    }

    @Override
    public Stream<QueryResult> stream() throws SQLException {
        return StreamUtilities.toStream(this.initial)
                .peek(this::peekQueryStatus)
                .map(SyncQueryStatusListener::extractQueryResult)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private static Optional<QueryResult> extractQueryResult(ExecuteQueryResponse response) {
        return Optional.ofNullable(response).map(ExecuteQueryResponse::getQueryResult);
    }

    private void peekQueryStatus(ExecuteQueryResponse response) {
        Optional.ofNullable(response)
                .map(ExecuteQueryResponse::getQueryInfo)
                .map(QueryInfo::getQueryStatus)
                .ifPresent(status::set);
    }

    @SneakyThrows
    private static String getQueryId(ExecuteQueryResponse response, String query) {
        val rootErrorMessage = "The server did not supply an ID for the query: " + query;
        return Optional.ofNullable(response)
                .map(ExecuteQueryResponse::getQueryInfo)
                .map(QueryInfo::getQueryStatus)
                .map(QueryStatus::getQueryId)
                .orElseThrow(() ->
                        new DataCloudJDBCException(rootErrorMessage, new IllegalStateException(rootErrorMessage)));
    }
}
