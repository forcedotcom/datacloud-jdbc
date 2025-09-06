/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.partial;

import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.util.Unstable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.QueryResult;

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
            log.info(
                    "Fetching chunk based query result stream. queryId={}, chunkId={}, limit={}",
                    queryId,
                    chunkId,
                    limitId);
            iterator = client.getQueryResult(queryId, chunkId.getAndIncrement(), omitSchema.getAndSet(true));
        }

        if (iterator.hasNext()) {
            return true;
        }

        if (chunkId.get() < limitId) {
            log.info(
                    "Fetching new chunk based query result stream. queryId={}, chunkId={}, limit={}",
                    queryId,
                    chunkId,
                    limitId);
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
