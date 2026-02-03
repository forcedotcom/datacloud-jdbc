/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultParam;

/**
 * Asynchronous iterator over a range of chunks from a query result.
 *
 * <p>This iterator fetches chunks on demand, providing natural backpressure.
 * It doesn't validate the range of chunks, so it is on the caller to ensure
 * that the result range is valid.</p>
 *
 * <p>No network calls are made during construction. Calls are only made when
 * {@link #next()} is invoked.</p>
 *
 * <p>This is the async equivalent of
 * {@link com.salesforce.datacloud.jdbc.protocol.ChunkRangeIterator}.</p>
 */
@Slf4j
public class AsyncChunkRangeIterator extends AsyncResultRangeIterator {

    /**
     * Creates an async iterator over a range of chunks.
     *
     * @param queryClient  The client for a specific query id
     * @param chunkId      The starting chunk id
     * @param limit        The number of chunks to fetch
     * @param omitSchema   Whether to omit schema in responses
     * @param outputFormat The output format for the results
     * @return A new AsyncChunkRangeIterator instance
     */
    public static AsyncChunkRangeIterator of(
            @NonNull QueryAccessGrpcClient queryClient,
            long chunkId,
            long limit,
            boolean omitSchema,
            @NonNull OutputFormat outputFormat) {
        return new AsyncChunkRangeIterator(queryClient, outputFormat, omitSchema, chunkId, chunkId + limit);
    }

    // The next chunk id to fetch
    private long chunkId;
    // The chunk id after the last chunk to fetch
    private final long limitChunkId;

    private AsyncChunkRangeIterator(
            QueryAccessGrpcClient client,
            OutputFormat outputFormat,
            boolean omitSchema,
            long chunkId,
            long limitChunkId) {
        super(client, outputFormat, omitSchema);
        this.chunkId = chunkId;
        this.limitChunkId = limitChunkId;
    }

    @Override
    protected boolean hasMoreToFetch() {
        return chunkId < limitChunkId;
    }

    @Override
    protected QueryResultParam buildQueryResultParam() {
        return client.getQueryResultParamBuilder()
                .setChunkId(chunkId++)
                .setOmitSchema(omitSchema)
                .setOutputFormat(outputFormat)
                .build();
    }

    @Override
    protected String buildLogMessage() {
        return String.format(
                "getQueryResult queryId=%s, chunkId=%d, limit=%d",
                client.getQueryId(), chunkId - 1, limitChunkId - chunkId + 1);
    }

    @Override
    protected CompletionStage<Optional<QueryResult>> handleEmptyFirstResult() {
        if ((chunkId == 1) && (chunkId < limitChunkId)) {
            // In special cases on adaptive timeout Hyper can produce an empty first chunk
            // We thus retry immediately with next chunk in this case
            iterator = null;
            return next();
        } else {
            log.error(
                    "Unexpected empty chunk, stopping iterator before limit. queryId={}, chunkId={}, limit={}",
                    client.getQueryId(),
                    chunkId - 1,
                    limitChunkId);
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }
}
