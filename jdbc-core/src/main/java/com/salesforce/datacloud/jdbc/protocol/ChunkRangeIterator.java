/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import com.salesforce.datacloud.jdbc.protocol.grpc.util.BufferingStreamIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultParam;

/**
 * See {@link ChunkRangeIterator#of(QueryAccessGrpcClient, long, long, boolean, OutputFormat)}
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ChunkRangeIterator implements Iterator<QueryResult> {
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
        return new ChunkRangeIterator(chunkId, chunkId + limit, omitSchema, queryClient, outputFormat, null);
    }

    private long chunkId;

    private final long limitChunkId;

    private boolean omitSchema;

    private final QueryAccessGrpcClient client;

    private final OutputFormat outputFormat;

    private BufferingStreamIterator<QueryResultParam, QueryResult> iterator;

    @Override
    public boolean hasNext() {
        // This is the no op case where we have a non-empty iterator
        if ((iterator != null) && iterator.hasNext()) {
            return true;
        }
        // This is the case where we have finished all chunks
        if (chunkId >= limitChunkId) {
            return false;
        }

        // Here we need to fetch a chunk
        val request = client.getQueryResultParamBuilder()
                .setChunkId(chunkId++)
                .setOmitSchema(omitSchema)
                .setOutputFormat(outputFormat)
                .build();
        val message = String.format(
                "getQueryResult queryId=%s, chunkId=%d, limit=%d",
                client.getQueryId(), chunkId, limitChunkId - chunkId);
        iterator = new BufferingStreamIterator<QueryResultParam, QueryResult>(message, log);
        client.getStub().getQueryResult(request, iterator.getObserver());

        if (iterator.hasNext()) {
            // Even if omitSchema was initially false we only need the schema for the first chunk in the result stream.
            if (!omitSchema) {
                omitSchema = true;
            }
            return true;
        } else if ((chunkId == 1) && (chunkId < limitChunkId)) {
            // In special cases on adaptive timeout Hyper can produce an empty first chunk
            // We thus retry immediately with next chunk in this case
            return hasNext();
        } else {
            log.error(
                    "Unexpected empty chunk, stopping iterator before limit. queryId={}, chunkId={}, limit{}",
                    client.getQueryId(),
                    chunkId,
                    limitChunkId);
            return false;
        }
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return iterator.next();
    }
}
