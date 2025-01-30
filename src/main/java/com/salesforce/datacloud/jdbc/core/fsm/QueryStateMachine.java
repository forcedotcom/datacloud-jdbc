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

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.core.listener.QueryStatusListener;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.hyperdb.grpc.QueryParam;
import com.salesforce.hyperdb.grpc.QueryResult;
import com.salesforce.hyperdb.grpc.QueryStatus;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;

public class QueryStateMachine implements Iterator<QueryResult>, QueryStatusListener {
    public QueryStateMachine(
            @NonNull HyperGrpcClientExecutor client,
            @NonNull String query,
            @NonNull QueryParam.TransferMode transferMode) {
        this.context = Context.builder().client(client).build();
        this.current = new ExecuteQueryState(query, transferMode);
        this.query = query;
    }

    public QueryStateMachine(
            @NonNull HyperGrpcClientExecutor client, @NonNull String queryId, int offset, int rowCount) {
        this.query = null;
        this.current = new CheckForMorePages();
        this.context = Context.builder()
                .client(client)
                .queryId(queryId)
                .index(new AtomicLong(offset))
                .rowsPerPage(rowCount)
                .build();
    }

    @NonNull private final Context context;

    @NonNull private State current;

    @Override
    public boolean hasNext() {
        while (!(current instanceof EmitQueryResult) && !(current instanceof Finished)) {
            current = current.transition(context);
        }

        if (current instanceof EmitQueryResult) {
            return ((EmitQueryResult) current).hasNext();
        }

        throw new IllegalStateException("It is illegal to not be Finished and not be IterableState");
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (current instanceof EmitQueryResult) {
            return ((EmitQueryResult) current).next();
        }

        throw new IllegalStateException("Non-IterableState types can never have a next");
    }

    @Getter
    private final String query;

    @Override
    public boolean isReady() {
        return Optional.ofNullable(context.getQueryStatus())
                .map(QueryStatus::getCompletionStatus)
                .map(t -> t == QueryStatus.CompletionStatus.RESULTS_PRODUCED
                        || t == QueryStatus.CompletionStatus.FINISHED)
                .orElse(false);
    }

    @Override
    public String getStatus() {
        return Optional.ofNullable(context.getQueryStatus())
                .map(QueryStatus::getCompletionStatus)
                .map(Enum::name)
                .orElse(null);
    }

    @Override
    public String getQueryId() {
        return context.getQueryId();
    }

    @Override
    public DataCloudResultSet generateResultSet() {
        return StreamingResultSet.of(this.query, this);
    }

    @Override
    public Stream<QueryResult> stream() throws SQLException {
        return StreamUtilities.toStream(this);
    }
}
