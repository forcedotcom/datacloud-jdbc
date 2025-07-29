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
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.Deadline;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;

@AllArgsConstructor
public class AsyncQueryResultIterator implements QueryResultIterator {
    private final HyperGrpcClientExecutor client;

    @Getter
    private final String queryId;

    public static AsyncQueryResultIterator of(String sql, HyperGrpcClientExecutor client, QueryTimeout timeout)
            throws SQLException {
        try {
            val result = client.executeAsyncQuery(sql, timeout).next();
            val queryId = result.getQueryInfo().getQueryStatus().getQueryId();
            return new AsyncQueryResultIterator(client, queryId);
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createQueryException(sql, ex);
        }
    }

    @Override
    public DataCloudQueryStatus getQueryStatus() throws DataCloudJDBCException {
        val small = Deadline.of(Duration.ofSeconds(5));
        return client.waitForQueryStatus(getQueryId(), small, s -> true);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @SneakyThrows
    @Override
    public QueryResult next() {
        return null;
    }
}
