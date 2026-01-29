/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.QueryAccessHandle;
import com.salesforce.datacloud.jdbc.protocol.async.util.AsyncIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.AsyncStreamObserverIterator;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Asynchronous iterator over query results.
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
// TODO: While this passes all tests this I didn't review yet after Windsurf generation and thus likely
// it's unnecessary complex
public class AsyncQueryResultIterator implements AsyncIterator<QueryResult>, QueryAccessHandle {

    private final AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse> executeQueryMessages;
    private final HyperServiceGrpc.HyperServiceStub executeQueryStub;
    private final OutputFormat outputFormat;
    // The query client is initialized when we receive the query id
    private QueryAccessGrpcClient queryClient;
    // The info iterator is initialized when we receive the query id
    private AsyncQueryInfoIterator infoMessages;

    private QueryResult next;

    @Getter
    private QueryStatus queryStatus;

    private long nextChunk;
    private AsyncChunkRangeIterator chunkIterator;

    /**
     * Initializes a new async query result iterator. Will start query execution.
     *
     * @param stub              the stub used to execute the gRPC calls
     * @param executeQueryParam the query parameters to execute
     * @return a new AsyncQueryResultIterator instance
     */
    public static AsyncQueryResultIterator of(HyperServiceGrpc.HyperServiceStub stub, QueryParam executeQueryParam) {
        String message = "executeQuery. mode=" + executeQueryParam.getTransferMode();
        AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse> iterator =
                new AsyncStreamObserverIterator<>(message, log);
        stub.executeQuery(executeQueryParam, iterator.getObserver());
        return new AsyncQueryResultIterator(
                iterator,
                stub,
                executeQueryParam.getOutputFormat(),
                null,
                null,
                null,
                null,
                executeQueryParam.getTransferMode() == QueryParam.TransferMode.ASYNC ? 0 : 1,
                null);
    }

    @Override
    public CompletionStage<Optional<QueryResult>> next() {
        // If we have a buffered result, return it
        if (next != null) {
            QueryResult result = next;
            next = null;
            return CompletableFuture.completedFuture(Optional.of(result));
        }
        return nextInternal();
    }

    private CompletionStage<Optional<QueryResult>> nextInternal() {
        // Try to get next from execute query stream first
        return tryExecuteQueryStream();
    }

    private CompletionStage<Optional<QueryResult>> tryExecuteQueryStream() {
        return executeQueryMessages
                .next()
                .handle((opt, err) -> new ResultOrError<>(opt, err))
                .thenCompose(result -> {
                    if (result.error != null) {
                        return handleExecuteQueryError(result.error);
                    }
                    if (result.value != null && result.value.isPresent()) {
                        return processExecuteQueryResponse(result.value.get());
                    }
                    // Execute query stream ended, continue with chunk fetching
                    return continueWithChunks();
                });
    }

    private CompletionStage<Optional<QueryResult>> handleExecuteQueryError(Throwable error) {
        Throwable cause = unwrapException(error);
        // Handle CANCELLED gracefully - indicates stream finished
        if (cause instanceof StatusRuntimeException) {
            StatusRuntimeException ex = (StatusRuntimeException) cause;
            if (ex.getStatus().getCode() == Status.Code.CANCELLED && queryStatus != null) {
                return continueWithChunks();
            }
        }
        CompletableFuture<Optional<QueryResult>> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    private CompletionStage<Optional<QueryResult>> processExecuteQueryResponse(ExecuteQueryResponse msg) {
        if (msg.hasQueryResult()) {
            return CompletableFuture.completedFuture(Optional.of(msg.getQueryResult()));
        } else if (msg.hasQueryInfo() && msg.getQueryInfo().hasQueryStatus()) {
            queryStatus = msg.getQueryInfo().getQueryStatus();
            // Initialize query client & info iterator
            if (queryClient == null) {
                queryClient = QueryAccessGrpcClient.of(queryStatus.getQueryId(), executeQueryStub);
                infoMessages = AsyncQueryInfoIterator.of(queryClient);
            }
        }
        // Continue fetching from execute query stream
        return tryExecuteQueryStream();
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
        if ((queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED)
                || (queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.RESULTS_PRODUCED)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // Need to poll for more info
        return pollForMoreChunks();
    }

    private CompletionStage<Optional<QueryResult>> pollForMoreChunks() {
        return infoMessages.next().thenCompose(opt -> {
            if (opt.isPresent()) {
                QueryInfo info = opt.get();
                if (info.hasQueryStatus()) {
                    queryStatus = info.getQueryStatus();
                }
                return continueWithChunks();
            }
            // Info stream ended unexpectedly, check if we're done
            boolean queryIsDone = (queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED)
                    || (queryStatus.getCompletionStatus() == QueryStatus.CompletionStatus.RESULTS_PRODUCED);
            boolean allChunksConsumed = (nextChunk >= queryStatus.getChunkCount()) && (chunkIterator == null);

            if (queryIsDone && allChunksConsumed) {
                return CompletableFuture.completedFuture(Optional.empty());
            }

            // This should never happen
            CompletableFuture<Optional<QueryResult>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Unexpected end in next()"));
            return future;
        });
    }

    private Throwable unwrapException(Throwable t) {
        if (t instanceof java.util.concurrent.CompletionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    /**
     * Helper class to carry either a result or an error through the async pipeline.
     */
    private static class ResultOrError<T> {
        final Optional<T> value;
        final Throwable error;

        ResultOrError(Optional<T> value, Throwable error) {
            this.value = value;
            this.error = error;
        }
    }

    @Override
    public void close() {
        executeQueryMessages.close();
        if (infoMessages != null) {
            infoMessages.close();
        }
        if (chunkIterator != null) {
            chunkIterator.close();
        }
    }
}
