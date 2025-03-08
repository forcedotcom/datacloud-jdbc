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
import com.salesforce.datacloud.jdbc.util.Unstable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Unstable
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ChunkBased implements Iterator<QueryResult> {
    public static ChunkBased of(
            @NonNull HyperGrpcClientExecutor client, @NonNull String queryId, long chunkId, long limit) {
        return new ChunkBased(client, queryId, new AtomicLong(chunkId), chunkId + limit);
    }

    @NonNull private final HyperGrpcClientExecutor client;

    @NonNull private final String queryId;

    private final AtomicLong chunkId;

    private final long limitId;

    private Iterator<QueryResult> iterator;

    @Override
    public boolean hasNext() {
        if (iterator == null) {
            iterator = client.getQueryResult(queryId, chunkId.getAndIncrement(), false);
        }

        if (iterator.hasNext()) {
            return true;
        }

        if (chunkId.get() < limitId) {
            iterator = client.getQueryResult(queryId, chunkId.getAndIncrement(), true);
        }

        return iterator.hasNext();
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return iterator.next();
    }
}
