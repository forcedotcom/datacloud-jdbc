/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.QueryAccessHandle;
import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncIterator;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Asynchronous iterator over the full query execution result.
 *
 * <p>This iterator handles the complete query execution lifecycle asynchronously:
 * executing the query, fetching results from the initial response stream, polling
 * for query info updates, and fetching additional chunks as they become available.</p>
 *
 * <p>This is the async equivalent of
 * {@link com.salesforce.datacloud.jdbc.protocol.QueryResultIterator}.</p>
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AsyncQueryResultIterator implements AsyncIterator<QueryResult>, QueryAccessHandle {

    // Iterator over the execute query stream (handles inline results and query info)
    private final AsyncExecuteQueryIterator executeQueryIterator;
    private final HyperServiceGrpc.HyperServiceStub stub;
    // The format of the result stream of the query
    private final OutputFormat outputFormat;

    // The query client is initialized when we receive the query id
    private QueryAccessGrpcClient queryClient;
    // The info iterator is initialized when we receive the query id
    private AsyncQueryInfoIterator infoMessages;

    // The next chunk to be fetched
    private long nextChunk;
    // The chunk iterator is initialized when we receive the query id, it covers the range of chunks known from the last
    // query info. But as the query proceeds new chunks might be added, so we need to reinitialize the iterator
    // accordingly when it is exhausted.
    private AsyncChunkRangeIterator chunkIterator;

    // Whether the execute query stream has been exhausted
    private boolean executeQueryStreamExhausted;

    // The latest query status (updated from both executeQuery and info polling)
    private QueryStatus queryStatus;

    /**
     * Initializes a new async query result iterator. Will start query execution.
     *
     * @param stub              the stub used to execute the gRPC calls
     * @param executeQueryParam the query parameters to execute
     * @return a new AsyncQueryResultIterator instance
     */
    public static AsyncQueryResultIterator of(HyperServiceGrpc.HyperServiceStub stub, QueryParam executeQueryParam) {
        AsyncExecuteQueryIterator executeQueryIterator = AsyncExecuteQueryIterator.of(stub, executeQueryParam);
        // With non async transfer modes the first chunk is returned inline and thus we start with chunk 1
        long nextChunk = executeQueryParam.getTransferMode() == QueryParam.TransferMode.ASYNC ? 0 : 1;
        return new AsyncQueryResultIterator(
                executeQueryIterator,
                stub,
                executeQueryParam.getOutputFormat(),
                null,
                null,
                nextChunk,
                null,
                false,
                null);
    }

    @Override
    public CompletionStage<Optional<QueryResult>> next() {
        // If execute query stream is not exhausted, try to get next from it
        if (!executeQueryStreamExhausted) {
            return AsyncQueryInfoIterator.handleCompose(executeQueryIterator.next(), (opt, error) -> {
                // Always try to update queryStatus
                if (executeQueryIterator.getQueryStatus() != null) {
                    queryStatus = executeQueryIterator.getQueryStatus();
                }

                if (error != null) {
                    CompletableFuture<Optional<QueryResult>> future = new CompletableFuture<>();
                    future.completeExceptionally(error);
                    return future;
                } else {
                    if (opt.isPresent()) {

                        return CompletableFuture.completedFuture(opt);
                    }
                    // Execute query stream ended, continue with chunk fetching
                    executeQueryStreamExhausted = true;
                    initializeQueryClientIfNeeded();
                    return continueWithChunks();
                }
            });
        }

        // Continue with chunk fetching
        return continueWithChunks();
    }

    private void initializeQueryClientIfNeeded() {
        if (queryClient == null) {
            queryStatus = executeQueryIterator.getQueryStatus();
            if (queryStatus != null) {
                queryClient = QueryAccessGrpcClient.of(queryStatus.getQueryId(), stub);
                infoMessages = AsyncQueryInfoIterator.of(queryClient);
            } else {
                // This should never happen if the RPC stream closes successfully
                throw new IllegalArgumentException("Query status not available");
            }
        }
    }

    private CompletionStage<Optional<QueryResult>> continueWithChunks() {
        // At this point queryClient is guaranteed to be initialized

        // If we have an active chunk iterator, try that first
        if (chunkIterator != null) {
            return chunkIterator.next().thenCompose(opt -> {
                if (opt.isPresent()) {
                    return CompletableFuture.completedFuture(opt);
                }
                // Chunk iterator exhausted
                chunkIterator = null;
                return continueWithChunks();
            });
        }

        // Check if we have more chunks to fetch
        if (queryStatus.getChunkCount() > nextChunk) {
            chunkIterator = AsyncChunkRangeIterator.of(
                    queryClient, nextChunk, queryStatus.getChunkCount() - nextChunk, true, outputFormat);
            nextChunk = queryStatus.getChunkCount();
            return continueWithChunks();
        }

        // Check if query is finished
        if (((queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED)
                || (queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.RESULTS_PRODUCED))) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // Need to poll for more info
        return pollForMoreChunks();
    }

    private CompletionStage<Optional<QueryResult>> pollForMoreChunks() {
        return infoMessages.next().thenCompose(opt -> {
            if (opt.isPresent()) {
                if (opt.get().hasQueryStatus()) {
                    queryStatus = opt.get().getQueryStatus();
                    return continueWithChunks();
                } else {
                    return pollForMoreChunks();
                }
            }

            // This should never happen, callers of pollForMoreChunks should not call it when the query is already
            // finished
            CompletableFuture<Optional<QueryResult>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Unexpected end in next()"));
            return future;
        });
    }

    @Override
    public void close() {
        executeQueryIterator.close();
        if (infoMessages != null) {
            infoMessages.close();
        }
        if (chunkIterator != null) {
            chunkIterator.close();
        }
    }

    @Override
    public QueryStatus getQueryStatus() {
        return queryStatus;
    }
}
