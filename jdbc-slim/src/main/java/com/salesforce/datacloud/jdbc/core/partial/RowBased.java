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

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Builder
class RowBasedContext {
    @NonNull private final HyperGrpcClientExecutor client;

    @NonNull private final String queryId;

    private final long offset;

    @Getter
    private final long limit;

    @Getter
    private final AtomicLong seen = new AtomicLong(0);

    public Iterator<QueryResult> getQueryResult(boolean omitSchema) {
        val currentOffset = offset + seen.get();
        val currentLimit = limit - seen.get();
        return client.getQueryResult(queryId, currentOffset, currentLimit, omitSchema);
    }
}

/**
 * Row based results can be acquired with a QueryId and a row range, the behavior of getting more rows is determined by the {@link RowBased.Mode}:
 * {@link RowBased.Mode#SINGLE_RPC} execute a single GetQueryResult calls, we will return whatever data is on this response if any is available
 * {@link RowBased.Mode#FULL_RANGE} execute as many GetQueryResult calls until all available rows are exhausted
 */
public interface RowBased extends Iterator<QueryResult> {
    enum Mode {
        SINGLE_RPC,
        FULL_RANGE
    }

    static RowBased of(
            @NonNull HyperGrpcClientExecutor client,
            @NonNull String queryId,
            long offset,
            long limit,
            @NonNull Mode mode) {
        val context = RowBasedContext.builder()
                .client(client)
                .queryId(queryId)
                .offset(offset)
                .limit(limit)
                .build();
        switch (mode) {
            case SINGLE_RPC:
                return RowBasedSingleRpc.builder()
                        .iterator(context.getQueryResult(false))
                        .build();
            case FULL_RANGE:
                return RowBasedFullRange.builder().context(context).build();
        }
        throw new IllegalArgumentException("Unknown mode not supported. mode=" + mode);
    }
}

@Builder
class RowBasedSingleRpc implements RowBased {
    private final Iterator<QueryResult> iterator;

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public QueryResult next() {
        return iterator.next();
    }
}

@Builder
class RowBasedFullRange implements RowBased {
    private final RowBasedContext context;

    private Iterator<QueryResult> iterator;

    @Override
    public boolean hasNext() {
        if (iterator == null) {
            iterator = context.getQueryResult(false);
            return iterator.hasNext();
        }

        if (iterator.hasNext()) {
            return true;
        }

        if (context.getSeen().get() < context.getLimit()) {
            iterator = context.getQueryResult(true);
        }

        return iterator.hasNext();
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        val next = iterator.next();
        context.getSeen().addAndGet(next.getResultPartRowCount());
        return next;
    }
}
