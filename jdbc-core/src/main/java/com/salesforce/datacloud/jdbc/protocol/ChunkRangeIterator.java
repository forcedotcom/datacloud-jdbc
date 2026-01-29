/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.protocol.async.AsyncChunkRangeIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.SyncIteratorAdapter;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;

/**
 * Synchronous iterator over a range of chunks from a query result.
 *
 * <p>This extends {@link SyncIteratorAdapter} wrapping {@link AsyncChunkRangeIterator} to provide
 * a blocking iterator interface for backward compatibility.</p>
 *
 * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
 * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
 * {@link ChunkRangeIterator#hasNext()} and {@link ChunkRangeIterator#next()} calls.</p>
 *
 * @see AsyncChunkRangeIterator
 */
@Slf4j
public class ChunkRangeIterator extends SyncIteratorAdapter<QueryResult> {

    /**
     * Provides an Iterator over a range of chunks. It doesn't validate the range of chunks, so it is on the caller to
     * ensure that the result range is valid. No network calls will be done as part of this method call, only once the
     * iterator is used.
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link ChunkRangeIterator#hasNext()} and {@link ChunkRangeIterator#next()} calls.</p>
     */
    public static ChunkRangeIterator of(
            @NonNull QueryAccessGrpcClient queryClient,
            long chunkId,
            long limit,
            boolean omitSchema,
            @NonNull OutputFormat outputFormat) {
        return new ChunkRangeIterator(
                AsyncChunkRangeIterator.of(queryClient, chunkId, limit, omitSchema, outputFormat));
    }

    private ChunkRangeIterator(AsyncChunkRangeIterator asyncIterator) {
        super(asyncIterator);
    }
}
