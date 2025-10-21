/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.logging.ElapsedLogger;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.*;

/**
 * See {@link QueryResultIterator#of(HyperServiceGrpc.HyperServiceBlockingStub, QueryParam)}
 *
 */
@Slf4j
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class QueryResultIterator implements Iterator<QueryResult>, QueryAccessHandle {

    private final Iterator<ExecuteQueryResponse> executeQueryMessages;
    private final HyperServiceGrpc.HyperServiceBlockingStub executeQueryStub;
    private final OutputFormat outputFormat;
    // The query client is initialized when we receive the query id
    private QueryAccessGrpcClient queryClient;
    // The info iterator is initialized when we receive the query id. By having an continuous instance across all chunk
    // fetching we can get the potential benefit of interleaving the server sending new query infos while we are
    // fetching new chunks
    private QueryInfoIterator infoMessages;

    private QueryResult next;

    @Getter
    private QueryStatus queryStatus;

    private long nextChunk;
    private ChunkRangeIterator chunkIterator;

    /**
     * Initializes a new query result iterator. Will start query execution and thus might throw a StatusRuntimeException.
     *
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link RowRangeIterator#hasNext()} and {@link RowRangeIterator#next()} calls.</p>
     *
     * @param stub - the stub used to execute the gRPC calls to fetch the results
     * @param executeQueryParam - the query parameters to execute
     * @return a new QueryResultIterator instance
     */
    public static QueryResultIterator of(HyperServiceGrpc.HyperServiceBlockingStub stub, QueryParam executeQueryParam) {
        val message = "executeQuery. mode=" + executeQueryParam.getTransferMode();
        return ElapsedLogger.logTimedValueNonThrowing(
                () -> {
                    val iterator = new QueryResultIterator(
                            stub.executeQuery(executeQueryParam),
                            stub,
                            executeQueryParam.getOutputFormat(),
                            null,
                            null,
                            null,
                            null,
                            executeQueryParam.getTransferMode() == QueryParam.TransferMode.ASYNC ? 0 : 1,
                            null);
                    return iterator;
                },
                message,
                log);
    }

    /**
     * This extends the normal hasNext() logic with graceful handling of the CANCELLED error code which indicates
     * that the stream has finished and that a new stream should be started.
     * @return whether there is a next element
     */
    private boolean hasNextWithCancel(Iterator<ExecuteQueryResponse> messages) {
        try {
            return messages.hasNext();
        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode() == Status.Code.CANCELLED && queryStatus != null) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean hasNext() {
        // We need to loop the internal logic until we have a next value or the query is finished. During one while
        // iteration
        // we either produce a new next value, initialize a new chunk iterator or update the status.
        while (true) {
            // There is an unconsumed next value
            if (next != null) {
                return true;
            } else if (hasNextWithCancel(executeQueryMessages)) {
                // Check if we are still in the execute query phase and consume that stream
                ExecuteQueryResponse msg = executeQueryMessages.next();
                if (msg.hasQueryResult()) {
                    next = msg.getQueryResult();
                    return true;
                } else if (msg.hasQueryInfo() && msg.getQueryInfo().hasQueryStatus()) {
                    queryStatus = msg.getQueryInfo().getQueryStatus();
                    // Initialize query client & info iterator
                    if (queryClient == null) {
                        queryClient = QueryAccessGrpcClient.of(queryStatus.getQueryId(), executeQueryStub);
                        infoMessages = QueryInfoIterator.of(queryClient);
                    }
                }
                // Restart the next iteration to fetch next message
            }
            // At this point the query client is guaranteed to be initialized as an ExecuteQuery response must always
            // either
            // produce a QueryStatus or fail with an exception.
            else if ((chunkIterator != null) && chunkIterator.hasNext()) {
                // Happy case where we have a chunk iterator
                next = chunkIterator.next();
                return true;
            } else if (queryStatus.getChunkCount() > nextChunk) {
                // If we know about unconsumed chunks, lets first process those
                chunkIterator = ChunkRangeIterator.of(
                        queryClient, nextChunk, queryStatus.getChunkCount() - nextChunk, true, outputFormat);
                nextChunk = queryStatus.getChunkCount();
                // Start the next loop iteration to fetch next chunk
            } else if ((queryStatus.getCompletionStatus() != QueryStatus.CompletionStatus.FINISHED)
                    && (queryStatus.getCompletionStatus() != QueryStatus.CompletionStatus.RESULTS_PRODUCED)) {
                // Update the status
                // There must be an new status update as the query has not been observed to be finished yet
                QueryInfo info = infoMessages.next();
                if (info.hasQueryStatus()) {
                    queryStatus = info.getQueryStatus();
                }
                // Start the next loop iteration to fetch next chunk
            } else {
                // In this case the query is finished / has all results consumed status and all chunks have been fetched
                boolean queryIsDone = (queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED)
                        || (queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.RESULTS_PRODUCED);
                boolean allChunksConsumed = (nextChunk >= queryStatus.getChunkCount())
                        // No partially consumed chunks
                        && (!hasNextWithCancel(executeQueryMessages))
                        && ((chunkIterator == null) || !chunkIterator.hasNext());
                // This should never happen and would be a severe bug
                if (!(queryIsDone && allChunksConsumed)) {
                    throw new RuntimeException("Unexpected end in hasNext()");
                }
                return false;
            }
        }
    }

    @Override
    public QueryResult next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        QueryResult result = next;
        next = null;
        return result;
    }
}
