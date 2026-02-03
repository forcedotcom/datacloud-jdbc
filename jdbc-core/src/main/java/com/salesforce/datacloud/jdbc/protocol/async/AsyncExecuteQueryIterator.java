/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncIterator;
import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncStreamObserverIterator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Asynchronous iterator over the execute query response stream.
 *
 * <p>This iterator processes {@link ExecuteQueryResponse} messages from the gRPC stream,
 * extracting {@link QueryResult} values and tracking the latest {@link QueryInfo}.</p>
 *
 * <p>The execute query stream can contain both inline results (for non-ASYNC transfer modes)
 * and query info updates. This class handles both cases and provides access to the latest
 * query status for downstream chunk fetching.</p>
 *
 * <p>CANCELLED errors are treated as normal stream completion, as the V3 protocol
 * uses cancellation to signal expected end-of-stream in case of server side RPC timeout.</p>
 */
@Slf4j
@AllArgsConstructor
public class AsyncExecuteQueryIterator implements AsyncIterator<QueryResult> {

    private final AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse> streamIterator;

    /** The latest query info received from the stream. */
    @Getter
    private QueryInfo queryInfo;

    /**
     * Creates a new iterator and starts the execute query call.
     *
     * @param stub              the gRPC stub for the HyperService
     * @param executeQueryParam the query parameters
     * @return a new AsyncExecuteQueryIterator
     */
    public static AsyncExecuteQueryIterator of(HyperServiceGrpc.HyperServiceStub stub, QueryParam executeQueryParam) {
        String message = "executeQuery. mode=" + executeQueryParam.getTransferMode();
        AsyncStreamObserverIterator<QueryParam, ExecuteQueryResponse> iterator =
                new AsyncStreamObserverIterator<>(message, log);
        stub.executeQuery(executeQueryParam, iterator.getObserver());
        return new AsyncExecuteQueryIterator(iterator, null);
    }

    /**
     * Returns the latest query status from the received query info.
     *
     * @return the latest query status, or null if no query info has been received yet
     */
    public QueryStatus getQueryStatus() {
        return queryInfo != null && queryInfo.hasQueryStatus() ? queryInfo.getQueryStatus() : null;
    }

    @Override
    public CompletionStage<Optional<QueryResult>> next() {
        return fetchNext();
    }

    private CompletionStage<Optional<QueryResult>> fetchNext() {
        return streamIterator
                .next()
                .thenCompose(opt -> {
                    if (opt.isPresent()) {
                        ExecuteQueryResponse response = opt.get();
                        if (response.hasQueryResult()) {
                            return CompletableFuture.completedFuture(Optional.of(response.getQueryResult()));
                        }

                        if (response.hasQueryInfo()) {
                            queryInfo = response.getQueryInfo();
                        }

                        // Not a result, fetch next message
                        return fetchNext();
                    }
                    // Stream ended normally
                    return CompletableFuture.completedFuture(Optional.empty());
                })
                .exceptionally(error -> {
                    // Handle errors - convert CANCELLED to end-of-stream if we have query info
                    Throwable cause;
                    if (error instanceof CompletionException && error.getCause() != null) {
                        cause = error.getCause();
                    } else {
                        cause = error;
                    }
                    if (cause instanceof StatusRuntimeException) {
                        StatusRuntimeException ex = (StatusRuntimeException) cause;
                        if (ex.getStatus().getCode() == Status.Code.CANCELLED && (queryInfo != null)) {
                            return Optional.empty();
                        }
                    }
                    // Re-throw other errors
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                });
    }

    @Override
    public void close() {
        streamIterator.close();
    }
}
