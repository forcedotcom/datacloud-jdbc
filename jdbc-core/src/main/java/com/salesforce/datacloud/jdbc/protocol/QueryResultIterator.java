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
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * QueryResultIterator using Composition Pattern.
 * Separates concerns into focused components for better maintainability and testability.
 * Components:
 * - QueryExecutor: Handles ExecuteQueryResponse messages and initialization
 * - ChunkProcessor: Manages chunk fetching and iteration
 * - StatusMonitor: Tracks query progress and status updates
 * - CompletionVerifier: Verifies query completion
 */
@Slf4j
public class QueryResultIterator implements Iterator<QueryResult>, QueryAccessHandle {
    private final QueryExecutor queryExecutor;
    private final ChunkProcessor chunkProcessor;
    private final StatusMonitor statusMonitor;
    private final CompletionVerifier completionVerifier;

    private QueryResult next;

    /**
     * Private constructor for Composition Pattern.
     * Components are created and injected during construction.
     */
    private QueryResultIterator(
            QueryExecutor queryExecutor,
            ChunkProcessor chunkProcessor,
            StatusMonitor statusMonitor,
            CompletionVerifier completionVerifier) {
        this.queryExecutor = queryExecutor;
        this.chunkProcessor = chunkProcessor;
        this.statusMonitor = statusMonitor;
        this.completionVerifier = completionVerifier;
    }

    /**
     * Creates a new QueryResultIterator using Composition Pattern.
     * Will start query execution and thus might throw a StatusRuntimeException.
     *
     * <p>Note: To set a timeout configure the stub in the client accordingly.</p>
     * <p>Attention: This iterator might throw {@link io.grpc.StatusRuntimeException} exceptions during
     * {@link QueryResultIterator#hasNext()} and {@link QueryResultIterator#next()} calls.</p>
     *
     * @param stub - the stub used to execute the gRPC calls to fetch the results
     * @param executeQueryParam - the query parameters to execute
     * @return a new QueryResultIterator instance
     */
    public static QueryResultIterator of(HyperServiceGrpc.HyperServiceBlockingStub stub, QueryParam executeQueryParam) {
        val message = "executeQuery. mode=" + executeQueryParam.getTransferMode();
        return ElapsedLogger.logTimedValueNonThrowing(
                () -> {
                    // Create focused components
                    QueryExecutor executor = new QueryExecutor(stub, executeQueryParam);
                    // Initialize nextChunk based on transfer mode (like original implementation)
                    long initialNextChunk =
                            executeQueryParam.getTransferMode() == QueryParam.TransferMode.ASYNC ? 0 : 1;
                    ChunkProcessor processor =
                            new ChunkProcessor(executeQueryParam.getOutputFormat(), initialNextChunk);
                    StatusMonitor monitor = new StatusMonitor();
                    CompletionVerifier verifier = new CompletionVerifier();

                    return new QueryResultIterator(executor, processor, monitor, verifier);
                },
                message,
                log);
    }

    @Override
    public boolean hasNext() {
        // We need to loop the internal logic until we have a next value or the query is finished.
        // During one while iteration we either produce a new next value, initialize a new chunk iterator or update the
        // status.
        while (true) {
            // There is an unconsumed next value
            if (next != null) {
                return true;
            }

            // Process ExecuteQuery messages directly in the main loop (like original implementation)
            if (queryExecutor.hasMoreMessages()) {
                ExecuteQueryResponse msg = queryExecutor.getNextMessage();
                if (msg.hasQueryResult()) {
                    next = msg.getQueryResult();
                    return true;
                } else if (msg.hasQueryInfo() && msg.getQueryInfo().hasQueryStatus()) {
                    QueryStatus status = msg.getQueryInfo().getQueryStatus();
                    queryExecutor.updateStatus(status);
                    // Initialize other components
                    statusMonitor.initialize(queryExecutor.getQueryClient(), status);
                    chunkProcessor.updateChunkRange(status, queryExecutor.getQueryClient());
                }
                // Restart the next iteration to fetch next message (like original implementation)
                continue;
            }

            // Ensure StatusMonitor has the current status from QueryExecutor
            statusMonitor.syncWithQueryExecutor(queryExecutor);

            // Only process chunks after ExecuteQuery stream is exhausted (like original implementation)
            if (chunkProcessor.tryGetNextChunk()) {
                next = chunkProcessor.getLastChunk();
                return true;
            }

            // Check if there are unconsumed chunks and create iterator if needed
            QueryStatus currentStatus = statusMonitor.getCurrentStatus();
            if (chunkProcessor.hasMoreChunks(currentStatus)) {
                chunkProcessor.updateChunkRange(currentStatus, queryExecutor.getQueryClient());
                continue; // Created new chunk iterator, continue the loop
            }

            // Only try to update status if the query is not finished yet
            boolean queryIsFinished = currentStatus != null
                    && (currentStatus.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED
                            || currentStatus.getCompletionStatus() == QueryStatus.CompletionStatus.RESULTS_PRODUCED);

            if (!queryIsFinished && statusMonitor.tryUpdateStatus()) {
                // Update chunk processor with new status
                QueryStatus newStatus = statusMonitor.getCurrentStatus();
                if (newStatus != null) {
                    chunkProcessor.updateChunkRange(newStatus, queryExecutor.getQueryClient());
                }
                continue; // Status updated, try again
            }

            // Only check completion if we've exhausted all other strategies
            if (completionVerifier.isCompleted(queryExecutor, chunkProcessor, statusMonitor)) {
                return false; // Query finished
            }

            // If we get here, it means none of the strategies found a result
            // and the query is not completed yet - this should not happen
            // This indicates a bug in our logic
            throw new RuntimeException("Unexpected state in hasNext() for queryId=" + queryExecutor.getQueryId());
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

    public String getQueryId() {
        return queryExecutor.getQueryId();
    }

    @Override
    public QueryStatus getQueryStatus() {
        return statusMonitor.getCurrentStatus();
    }

    private static class QueryExecutor {
        private final Iterator<ExecuteQueryResponse> messages;
        private final HyperServiceGrpc.HyperServiceBlockingStub stub;
        private QueryStatus queryStatus;
        private QueryAccessGrpcClient queryClient;

        QueryExecutor(HyperServiceGrpc.HyperServiceBlockingStub stub, QueryParam param) {
            this.stub = stub;
            this.messages = stub.executeQuery(param);
        }

        boolean hasMoreMessages() {
            return hasNextWithCancel(messages);
        }

        ExecuteQueryResponse getNextMessage() {
            return messages.next();
        }

        void updateStatus(QueryStatus status) {
            queryStatus = status;
            // Initialize query client when we receive the query id
            if (queryClient == null && !status.getQueryId().isEmpty()) {
                queryClient = QueryAccessGrpcClient.of(status.getQueryId(), stub);
            }
        }

        String getQueryId() {
            return queryStatus != null ? queryStatus.getQueryId() : null;
        }

        QueryAccessGrpcClient getQueryClient() {
            return queryClient;
        }

        QueryStatus getQueryStatus() {
            return queryStatus;
        }

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
    }

    /**
     * Manages chunk fetching and iteration.
     * Encapsulates the logic for creating and managing ChunkRangeIterator instances.
     */
    private static class ChunkProcessor {
        private final OutputFormat outputFormat;
        private ChunkRangeIterator chunkIterator;
        private QueryResult lastChunk;
        private long nextChunk;

        ChunkProcessor(OutputFormat outputFormat, long initialNextChunk) {
            this.outputFormat = outputFormat;
            this.nextChunk = initialNextChunk;
        }

        boolean tryGetNextChunk() {
            if (chunkIterator != null && chunkIterator.hasNext()) {
                lastChunk = chunkIterator.next();
                return true;
            }
            return false;
        }

        QueryResult getLastChunk() {
            return lastChunk;
        }

        void updateChunkRange(QueryStatus status, QueryAccessGrpcClient client) {
            if (status != null && client != null && status.getChunkCount() > nextChunk) {
                chunkIterator = ChunkRangeIterator.of(
                        client, nextChunk, status.getChunkCount() - nextChunk, true, outputFormat);
                nextChunk = status.getChunkCount();
            }
        }

        boolean hasMoreChunks(QueryStatus status) {
            if (status == null) {
                return false;
            }
            // Check if there are unconsumed chunks or partially consumed chunks
            return (nextChunk < status.getChunkCount()) || (chunkIterator != null && chunkIterator.hasNext());
        }
    }

    /**
     * Tracks query progress and status updates.
     * Encapsulates the logic for monitoring query status via QueryInfoIterator.
     */
    private static class StatusMonitor {
        private QueryInfoIterator infoIterator;
        private QueryStatus currentStatus;

        boolean tryUpdateStatus() {
            if (infoIterator == null || currentStatus == null) {
                return false;
            }

            // Process all available messages until we find one with a status
            while (infoIterator.hasNext()) {
                QueryInfo info = infoIterator.next();
                if (info.hasQueryStatus()) {
                    currentStatus = info.getQueryStatus();
                    return true;
                }
                // Skip optional messages without status and continue
            }
            return false;
        }

        QueryStatus getCurrentStatus() {
            return currentStatus;
        }

        void syncWithQueryExecutor(QueryExecutor executor) {
            if (currentStatus == null && executor.getQueryStatus() != null) {
                initialize(executor.getQueryClient(), executor.getQueryStatus());
            }
        }

        void initialize(QueryAccessGrpcClient client, QueryStatus status) {
            if (client != null && status != null) {
                if (infoIterator == null) {
                    this.infoIterator = QueryInfoIterator.of(client);
                }
                this.currentStatus = status; // Always update the current status
            }
        }
    }

    /**
     * Verifies query completion.
     * Encapsulates the logic for determining when a query is fully completed.
     */
    private static class CompletionVerifier {
        boolean isCompleted(QueryExecutor executor, ChunkProcessor processor, StatusMonitor monitor) {
            QueryStatus status = monitor.getCurrentStatus();
            if (status == null) {
                return false; // No status yet, not finished
            }

            boolean queryIsDone = (status.getCompletionStatus() == QueryStatus.CompletionStatus.FINISHED)
                    || (status.getCompletionStatus() == QueryStatus.CompletionStatus.RESULTS_PRODUCED);

            if (queryIsDone) {
                // For finished queries, check if all chunks are consumed
                return !executor.hasMoreMessages() && !processor.hasMoreChunks(status);
            }

            // Query is not completed yet, continue processing
            return false;
        }
    }
}
