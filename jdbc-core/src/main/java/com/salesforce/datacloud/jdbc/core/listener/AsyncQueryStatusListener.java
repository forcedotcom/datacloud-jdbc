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
import com.salesforce.datacloud.jdbc.core.partial.ChunkBased;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
@Deprecated
public class AsyncQueryStatusListener implements QueryStatusListener {
    @Getter
    private final String queryId;

    private final HyperGrpcClientExecutor client;

    private final Duration timeout;

    public static AsyncQueryStatusListener of(String query, HyperGrpcClientExecutor client, Duration timeout)
            throws SQLException {
        try {
            val result = client.executeAsyncQuery(query).next();
            val queryId = result.getQueryInfo().getQueryStatus().getQueryId();

            log.info("Executing async query. queryId={}, timeout={}", queryId, timeout);

            return AsyncQueryStatusListener.builder()
                    .queryId(queryId)
                    .client(client)
                    .timeout(timeout)
                    .build();
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createQueryException(query, ex);
        }
    }

    @Override
    public DataCloudResultSet generateResultSet() throws DataCloudJDBCException {
        return StreamingResultSet.of(queryId, client, stream().iterator());
    }

    @Override
    public Stream<QueryResult> stream() throws DataCloudJDBCException {
        val status = client.waitForQueryStatus(queryId, timeout, DataCloudQueryStatus::allResultsProduced);
        val iterator = ChunkBased.of(client, queryId, 0, status.getChunkCount(), false);

        return StreamUtilities.toStream(iterator);
    }
}
