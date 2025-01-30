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

import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.hyperdb.grpc.QueryInfo;
import com.salesforce.hyperdb.grpc.QueryParam;
import com.salesforce.hyperdb.grpc.QueryResult;
import com.salesforce.hyperdb.grpc.QueryStatus;
import java.util.Iterator;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

interface State {
    State transition(Context context);
}

@AllArgsConstructor
class ExecuteQueryState implements State {
    private final String sql;
    private final QueryParam.TransferMode transferMode;

    @SneakyThrows
    @Override
    public State transition(Context context) {
        val response = context.getClient().execute(sql, transferMode);

        if (!response.hasNext()) {
            return new Finished();
        }

        val first = response.next();
        context.peekQueryStatusAndExtractQueryResult(first);

        val iterator = StreamUtilities.toStream(response)
                .map(context::peekQueryStatusAndExtractQueryResult)
                .iterator();

        switch (transferMode) {
            case SYNC:
                return new EmitQueryResult(Long.MAX_VALUE, iterator);
            case ASYNC:
                return new CheckForMorePages();
            case ADAPTIVE:
                val offset = context.isRowBasedPagination() ? context.getRowsPerPage() : 1;
                context.getOmitSchema().set(true);
                return new EmitQueryResult(offset, iterator);
            default:
                throw new IllegalArgumentException(
                        "Unknown transfer mode is not supported. transferMode=" + transferMode.name());
        }
    }
}

@AllArgsConstructor
class EmitQueryResult implements State, Iterator<QueryResult> {
    private final long offset;
    private final Iterator<QueryResult> iterator;

    @Override
    public State transition(Context context) {
        if (iterator.hasNext()) {
            return this;
        }

        context.getIndex().set(offset);

        return new CheckForMorePages();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public QueryResult next() {
        return iterator.next();
    }
}

@NoArgsConstructor
class FetchMorePages implements State {
    @Override
    public State transition(Context context) {
        val queryId = context.getQueryId();
        val omitSchema = context.getOmitSchema().getAndSet(true);

        return context.isRowBasedPagination()
                ? new FetchMorePagesRowBased(queryId, omitSchema)
                : new FetchMorePagesChunkBased(queryId, omitSchema);
    }

    @AllArgsConstructor
    private static class FetchMorePagesChunkBased implements State {
        private final String queryId;
        private final boolean omitSchema;

        @Override
        public State transition(Context context) {
            if (context.getIndex().get() < context.getMax()) {
                val iterator = context.getClient()
                        .getQueryResult(queryId, context.getIndex().getAndIncrement(), omitSchema);
                return new EmitQueryResult(1, iterator);
            }

            return new Finished();
        }
    }

    @AllArgsConstructor
    private static class FetchMorePagesRowBased implements State {
        private final String queryId;
        private final boolean omitSchema;

        @Override
        public State transition(Context context) {
            val offset = Math.max(
                    context.getRowsPerPage(),
                    context.getMax() - context.getIndex().get()); // TODO: off by one error here?
            if (offset > 0) {
                val iterator = context.getClient()
                        .getQueryResult(queryId, context.getIndex().get(), offset, omitSchema);
                return new EmitQueryResult(offset, iterator);
            }

            return new Finished();
        }
    }
}

@NoArgsConstructor
class CheckForMorePages implements State {
    @Override
    public State transition(Context context) {

        val statusStream = context.streamQueryInfo()
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(QueryInfo::hasQueryStatus)
                .map(QueryInfo::getQueryStatus);

        val status = StreamUtilities.takeWhile(statusStream, s -> keepCheckingQueryStatus(s, context))
                .findFirst();

        return status.map(s -> decideTransitionFrom(s, context))
                .orElse(this); // TODO: how many times should we let check for more pages happen?
    }

    private static boolean keepCheckingQueryStatus(QueryStatus status, Context context) {
        switch (status.getCompletionStatus()) {
            case FINISHED:
                return false;
            case RESULTS_PRODUCED:
                val newMaximum = context.isRowBasedPagination() ? status.getRowCount() : status.getChunkCount();
                return context.getMax()
                        < newMaximum; // TODO: do we want to walk through results eagerly here or should we keep
                // iterating until FINISHED?
            default:
                return true;
        }
    }

    private static State decideTransitionFrom(QueryStatus status, Context context) {
        context.setQueryStatus(status);

        if (context.getIndex().get() < context.getMax()) {
            return new FetchMorePages();
        }

        if (status.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED) {
            return new Finished();
        }

        return null;
    }
}

@NoArgsConstructor
class Finished implements State {
    @Override
    public State transition(Context context) {
        throw new IllegalStateException("Cannot transition away from the Finished state");
    }
}
