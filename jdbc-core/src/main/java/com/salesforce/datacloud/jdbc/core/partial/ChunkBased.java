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
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.QueryResult;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Unstable
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ChunkBased implements Iterator<QueryResult> {
    public static ChunkBased of(
            @NonNull HyperGrpcClientExecutor client, @NonNull String queryId, long chunkId, long limit) {
        return ChunkBased.of(client, queryId, chunkId, limit, false);
    }

    public static ChunkBased of(
            @NonNull HyperGrpcClientExecutor client,
            @NonNull String queryId,
            long chunkId,
            long limit,
            boolean omitSchema) {
        return new ChunkBased(client, queryId, new AtomicLong(chunkId), chunkId + limit, new AtomicBoolean(omitSchema));
    }

    @NonNull private final HyperGrpcClientExecutor client;

    @NonNull private final String queryId;

    private final AtomicLong chunkId;

    private final long limitId;

    private Iterator<QueryResult> iterator;

    private final AtomicBoolean omitSchema;

    @Override
    public boolean hasNext() {
        if (iterator == null) {
            log.info("Fetching chunk based query result stream. queryId={}, chunkId={}, limit={}", queryId, chunkId, limitId);
            iterator = client.getQueryResult(queryId, chunkId.getAndIncrement(), omitSchema.getAndSet(true));
        }

        if (iterator.hasNext()) {
            return true;
        }

        if (chunkId.get() < limitId) {
            log.info("Fetching new chunk based query result stream. queryId={}, chunkId={}, limit={}", queryId, chunkId, limitId);
            iterator = client.getQueryResult(queryId, chunkId.getAndIncrement(), omitSchema.get());
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
