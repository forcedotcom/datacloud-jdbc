/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.async.util.AsyncIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.AsyncStreamObserverIterator;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Asynchronous iterator over QueryInfo messages of a Query.
 *
 * <p>This iterator keeps iterating until the query is finished. For finished queries it'll do
 * at most a single RPC call and return all infos returned from that call.</p>
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AsyncQueryInfoIterator implements AsyncIterator<QueryInfo> {
    /**
     * Creates a new async iterator over QueryInfo messages for the given query.
     *
     * @param queryClient The client for a specific query id
     * @return A new AsyncQueryInfoIterator instance
     */
    public static AsyncQueryInfoIterator of(@NonNull QueryAccessGrpcClient queryClient) {
        return new AsyncQueryInfoIterator(queryClient, null, false, 0);
    }

    /** The gRPC client for the specific query being polled. */
    private final QueryAccessGrpcClient client;
    /** The current gRPC stream iterator, null if no active stream. */
    private AsyncStreamObserverIterator<QueryInfoParam, QueryInfo> iterator;
    /** Whether the query has reached FINISHED completion status. */
    private boolean isQueryFinished;
    /** The current retry count, reset to 0 on successful message receipt. */
    private int retryCount;

    @Override
    public CompletionStage<Optional<QueryInfo>> next() {
        return nextInternal();
    }

    private CompletionStage<Optional<QueryInfo>> nextInternal() {
        // If we're finished, return empty
        if (isQueryFinished) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // If we don't have an iterator yet, create one
        if (iterator == null) {
            startNewGrpcCall();
        }

        // Request next from current iterator
        val fetchResult = iterator.next();
        val retriedResult = handleCompose(fetchResult, this::attemptRetry);
        return handleCompose(retriedResult, this::processResult);
    }

    /**
     * Attempts to retry on CANCELLED errors, up to 2 retries.
     * If no retry is needed or possible, returns the result or error as-is.
     */
    private CompletionStage<Optional<QueryInfo>> attemptRetry(Optional<QueryInfo> result, Throwable error) {
        // No need to retry
        if (result != null) {
            return CompletableFuture.completedFuture(result);
        }

        // Check the error and retry if possible
        Throwable cause;
        if (error instanceof CompletionException && error.getCause() != null) {
            cause = error.getCause();
        } else {
            cause = error;
        }
        if (cause instanceof StatusRuntimeException) {
            StatusRuntimeException ex = (StatusRuntimeException) cause;
            boolean isRetryable = ex.getStatus().getCode() == Status.Code.CANCELLED && retryCount < 2;
            if (isRetryable) {
                ++retryCount;
                iterator = null;
                // This will recursively call nextInternal() again but is bounded by the
                // retry count. If this returns failure the retries are exhausted
                return nextInternal();
            }
        }

        // No more retry budget, forward the current error
        CompletableFuture<Optional<QueryInfo>> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    /**
     * Processes a successful result from the stream.
     * Updates the finished flag based on query status and handles stream end by reconnecting if needed.
     */
    private CompletionStage<Optional<QueryInfo>> processResult(Optional<QueryInfo> value, Throwable error) {
        // If there is an error we forward it
        if (error != null) {
            CompletableFuture<Optional<QueryInfo>> future = new CompletableFuture<>();
            future.completeExceptionally(error);
            return future;
        }

        // Process the value
        if (value.isPresent()) {
            retryCount = 0;
            QueryInfo info = value.get();
            if (info.hasQueryStatus()) {
                isQueryFinished =
                        (info.getQueryStatus().getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED);
            }
            return CompletableFuture.completedFuture(Optional.of(info));
        } else {
            // Stream ended without value
            if (isQueryFinished) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            // Need to start a new stream
            ++retryCount;
            iterator = null;
            return nextInternal();
        }
    }

    /**
     * Utility function to handle the result of a {@link CompletionStage} and then compose the result into another
     * {@link CompletionStage}.
     *
     * <p>This is similar to CompletionStage#handle()} but handles the case where the provided
     * BiFunction returns a {@link CompletionStage} that may complete exceptionally.</p>
     */
    public static <T, U> CompletionStage<U> handleCompose(
            CompletionStage<T> source,
            java.util.function.BiFunction<? super T, Throwable, ? extends CompletionStage<U>> fn) {

        return source.handle(fn).thenCompose(Function.identity());
    }

    /**
     * Starts a new streaming gRPC call to fetch query info updates.
     */
    private void startNewGrpcCall() {
        QueryInfoParam request =
                client.getQueryInfoParamBuilder().setStreaming(true).build();
        String message = String.format("getQueryInfo queryId=%s, streaming=%s", client.getQueryId(), true);
        iterator = new AsyncStreamObserverIterator<>(message, log);
        client.getStub().getQueryInfo(request, iterator.getObserver());
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }
}
