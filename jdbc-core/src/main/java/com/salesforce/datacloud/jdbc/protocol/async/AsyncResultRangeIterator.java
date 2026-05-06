/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async;

import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncIterator;
import com.salesforce.datacloud.jdbc.protocol.async.core.AsyncStreamObserverIterator;
import com.salesforce.datacloud.jdbc.protocol.async.core.Step;
import com.salesforce.datacloud.jdbc.protocol.grpc.QueryAccessGrpcClient;
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
 *
 * <p>When this iterator needs to start a new {@code getQueryResult} stream, it does NOT call
 * the gRPC stub from inside the {@link CompletionStage} chain (which would run on a gRPC
 * executor thread). Instead, it emits {@link Step.NeedDispatch}; the synchronous pump then
 * runs the stub call on the caller thread so {@link io.grpc.ClientInterceptor#interceptCall}
 * {@code start} callbacks observe caller-thread {@link ThreadLocal}s.</p>
 *
 * <p>This iterator is not safe for concurrent calls to {@link #next()}; callers must wait for
 * the previous {@link CompletionStage} to complete before calling again, per the
 * {@link AsyncIterator#next()} contract. The {@code awaitingFirstResponseFromNew} flag and
 * other instance fields are mutated without synchronization and rely on the happens-before
 * relationship established by completion of the prior stage.</p>
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
    /**
     * Marker that the next response observed will be the first from a freshly created iterator.
     * Set when {@link Step.NeedDispatch} is emitted for a new stream, cleared as soon as the
     * stream produces a step. Used to drive {@link #handleEmptyFirstResult()} when the new
     * stream produces no data.
     */
    private boolean awaitingFirstResponseFromNew;

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
     * <p>The default implementation returns {@link Step#done()}, signaling end of iteration.
     * Subclasses can override to implement retry logic (e.g., for empty first chunks).</p>
     *
     * @return a CompletionStage with the result to return
     */
    protected CompletionStage<Step<QueryResult>> handleEmptyFirstResult() {
        return CompletableFuture.completedFuture(Step.<QueryResult>done());
    }

    @Override
    public CompletionStage<Step<QueryResult>> next() {
        return fetchNext();
    }

    /**
     * Fetches the next result, emitting {@link Step.NeedDispatch} when a new gRPC stream needs
     * to be started.
     */
    private CompletionStage<Step<QueryResult>> fetchNext() {
        // If no active iterator, ask the pump to dispatch a new gRPC call on the caller thread.
        if (iterator == null) {
            if (!hasMoreToFetch()) {
                return CompletableFuture.completedFuture(Step.<QueryResult>done());
            }
            // Build request and create the receiving observer/iterator now (so the observer is
            // wired before the stub call). The stub call itself happens inside the dispatch
            // thunk, which the synchronous pump executes on the caller thread.
            final QueryResultParam param = buildQueryResultParam();
            final AsyncStreamObserverIterator<QueryResultParam, QueryResult> newIter =
                    new AsyncStreamObserverIterator<>(buildLogMessage(), log);
            iterator = newIter;
            awaitingFirstResponseFromNew = true;
            Runnable dispatch = () -> client.getStub().getQueryResult(param, newIter.getObserver());
            return CompletableFuture.completedFuture(Step.<QueryResult>needDispatch(dispatch));
        }

        boolean firstFromNew = awaitingFirstResponseFromNew;
        awaitingFirstResponseFromNew = false;
        return iterator.next().thenCompose(step -> {
            if (step instanceof Step.Value) {
                QueryResult result = ((Step.Value<QueryResult>) step).getItem();
                onResultReceived(result);
                if (!omitSchema) {
                    omitSchema = true;
                }
                return CompletableFuture.completedFuture(Step.<QueryResult>value(result));
            } else if (step instanceof Step.NeedDispatch) {
                return CompletableFuture.completedFuture(
                        Step.<QueryResult>retypeNeedDispatch((Step.NeedDispatch<?>) step));
            } else if (step instanceof Step.Done) {
                // Current stream exhausted.
                iterator = null;
                if (firstFromNew) {
                    return handleEmptyFirstResult();
                }
                // Try to fetch more from a new iterator (which may emit NeedDispatch).
                return fetchNext();
            }
            throw new IllegalStateException("Unknown Step subtype: " + step.getClass());
        });
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }
}
