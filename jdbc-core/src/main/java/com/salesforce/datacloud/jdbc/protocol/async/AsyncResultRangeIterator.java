/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.async.util.AsyncIterator;
import com.salesforce.datacloud.jdbc.protocol.async.util.AsyncStreamObserverIterator;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultParam;

/**
 * Base class for asynchronous iterators over query result ranges.
 *
 * <p>This class provides the common iteration logic for fetching query results,
 * with hooks for subclasses to customize request building and result observation.</p>
 *
 * <p>Subclasses must implement:</p>
 * <ul>
 *   <li>{@link #hasMoreToFetch()} - determine if more results should be fetched</li>
 *   <li>{@link #buildQueryResultParam()} - build the gRPC request parameters</li>
 *   <li>{@link #buildLogMessage()} - create a log message for the fetch operation</li>
 * </ul>
 *
 * <p>Subclasses may optionally override:</p>
 * <ul>
 *   <li>{@link #onResultReceived(QueryResult)} - observe received results (e.g., to track progress)</li>
 *   <li>{@link #handleEmptyFirstResult()} - customize behavior when new iterator returns empty</li>
 * </ul>
 *
 * <p>The returned {@link CompletionStage} from {@link #next()} may complete exceptionally with
 * {@link io.grpc.StatusRuntimeException} if a gRPC error occurs.</p>
 */
@Slf4j
public abstract class AsyncResultRangeIterator implements AsyncIterator<QueryResult> {

    /** The gRPC client for the specific query being iterated. */
    protected final QueryAccessGrpcClient client;
    /** The output format for the query results. */
    protected final OutputFormat outputFormat;
    /** Whether to omit schema in responses. Set to true after the first result is received. */
    protected boolean omitSchema;
    /** The current gRPC stream iterator, null if no active stream. */
    protected AsyncStreamObserverIterator<QueryResultParam, QueryResult> iterator;

    protected AsyncResultRangeIterator(QueryAccessGrpcClient client, OutputFormat outputFormat, boolean omitSchema) {
        this.client = client;
        this.outputFormat = outputFormat;
        this.omitSchema = omitSchema;
    }

    /**
     * Checks whether there are more results to fetch.
     *
     * @return true if more results should be fetched, false if iteration is complete
     */
    protected abstract boolean hasMoreToFetch();

    /**
     * Builds the query result parameters for the next gRPC call.
     *
     * <p>Implementations should use {@link #client}'s {@code getQueryResultParamBuilder()}
     * and configure it appropriately (e.g., with chunk ID or row range).</p>
     *
     * @return the QueryResultParam for the next fetch operation
     */
    protected abstract QueryResultParam buildQueryResultParam();

    /**
     * Builds a descriptive log message for the current fetch operation.
     *
     * @return a log message describing the fetch operation
     */
    protected abstract String buildLogMessage();

    /**
     * Called when a result is received from the stream.
     *
     * <p>Subclasses can override this to track progress (e.g., update row offset based
     * on the number of rows in the result). The default implementation does nothing.</p>
     *
     * @param result the received query result
     */
    protected void onResultReceived(QueryResult result) {
        // Default: no-op. Subclasses can override to track progress.
    }

    /**
     * Handles the case when a newly created iterator returns empty on first request.
     *
     * <p>The default implementation returns empty, signaling end of iteration.
     * Subclasses can override to implement retry logic (e.g., for empty first chunks).</p>
     *
     * @return a CompletionStage with the result to return
     */
    protected CompletionStage<Optional<QueryResult>> handleEmptyFirstResult() {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletionStage<Optional<QueryResult>> next() {
        return fetchNext(false);
    }

    /**
     * Fetches the next result, creating a new gRPC stream if needed.
     *
     * @param isFirstFromNewIterator true if this is the first fetch from a newly created iterator
     * @return a CompletionStage with the next result or empty if iteration is complete
     */
    private CompletionStage<Optional<QueryResult>> fetchNext(boolean isFirstFromNewIterator) {
        // If no active iterator, try to create one
        if (iterator == null) {
            if (!hasMoreToFetch()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            // Build request and create new iterator
            QueryResultParam param = buildQueryResultParam();
            iterator = new AsyncStreamObserverIterator<>(buildLogMessage(), log);
            client.getStub().getQueryResult(param, iterator.getObserver());
            isFirstFromNewIterator = true;
        }

        boolean firstFromNew = isFirstFromNewIterator;
        return iterator.next().thenCompose(opt -> {
            if (opt.isPresent()) {
                onResultReceived(opt.get());
                if (!omitSchema) {
                    omitSchema = true;
                }
                return CompletableFuture.completedFuture(opt);
            }
            // Iterator exhausted
            iterator = null;
            if (firstFromNew) {
                return handleEmptyFirstResult();
            }
            // Try to fetch more from a new iterator
            return fetchNext(false);
        });
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }
}
