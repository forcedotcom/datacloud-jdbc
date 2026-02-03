/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStubProvider;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.async.AsyncQueryResultIterator;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;

/**
 * This example demonstrates how to use {@link AsyncQueryResultIterator} directly
 * for asynchronous iteration over query results using CompletionStage.
 */
@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class AsyncQueryResultIteratorTest {
    /**
     * This example shows fully async-style iteration using CompletionStage chaining.
     * No blocking calls are made until the final join() - all iteration logic runs
     * within the async pipeline.
     */
    @Test
    public void testFullyAsyncIteration() {
        assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();

            val queryParam = QueryParam.newBuilder()
                    .setQuery("SELECT s FROM generate_series(1, 100) s")
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .setTransferMode(QueryParam.TransferMode.SYNC)
                    .build();

            try (val iterator = AsyncQueryResultIterator.of(stub, queryParam)) {
                val chunkCount = new AtomicInteger(0);

                // Fully async iteration using recursive CompletionStage chaining
                CompletionStage<Void> iteration = consumeAllAsync(iterator, chunkCount);

                // Only block at the very end to wait for completion
                iteration.toCompletableFuture().join();

                log.info("Async iteration completed. Total chunks: {}", chunkCount.get());
                assertThat(chunkCount.get()).isGreaterThan(0);
            }
        });
    }

    /**
     * Recursively consumes all results from the iterator asynchronously.
     * Each call to next() returns a CompletionStage that, when complete,
     * chains to the next iteration.
     */
    private CompletionStage<Void> consumeAllAsync(AsyncQueryResultIterator iterator, AtomicInteger chunkCount) {
        return iterator.next().thenCompose(opt -> {
            if (!opt.isPresent()) {
                // End of results - return completed future
                return CompletableFuture.completedFuture(null);
            }

            // Process this chunk
            val count = chunkCount.incrementAndGet();
            log.info("Async: Received chunk {}", count);

            // Chain to next iteration (recursive async call)
            return consumeAllAsync(iterator, chunkCount);
        });
    }

    /**
     * This example shows how to handle errors in async iteration by converting
     * throwables to SQLException/DataCloudJDBCException using QueryExceptionHandler.
     */
    @Test
    public void testAsyncIterationWithErrorHandling() {
        assertWithStubProvider(stubProvider -> {
            val stub = stubProvider.getStub();

            // Use an invalid query to trigger an error
            val queryParam = QueryParam.newBuilder()
                    .setQuery("SELECT * FROM non_existent_table")
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .setTransferMode(QueryParam.TransferMode.SYNC)
                    .build();

            try (val iterator = AsyncQueryResultIterator.of(stub, queryParam)) {
                val chunkCount = new AtomicInteger(0);
                val errorHolder = new AtomicReference<SQLException>();

                // Async iteration with error handling
                CompletionStage<Void> iteration =
                        consumeAllWithErrorHandling(iterator, queryParam.getQuery(), chunkCount, errorHolder);

                // Wait for completion
                iteration.toCompletableFuture().join();

                // Check if an error was captured
                if (errorHolder.get() != null) {
                    SQLException sqlException = errorHolder.get();
                    log.info("Caught SQLException: {}", sqlException.getMessage());
                    log.info("SQLState: {}", sqlException.getSQLState());
                    assertThat(sqlException.getSQLState()).isNotNull();
                }
            }
        });
    }

    /**
     * Consumes all results with proper error handling, converting exceptions
     * to SQLException via QueryExceptionHandler.
     */
    private CompletionStage<Void> consumeAllWithErrorHandling(
            AsyncQueryResultIterator iterator,
            String query,
            AtomicInteger chunkCount,
            AtomicReference<SQLException> errorHolder) {

        return iterator.next()
                .thenCompose(opt -> {
                    if (!opt.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    chunkCount.incrementAndGet();
                    return consumeAllWithErrorHandling(iterator, query, chunkCount, errorHolder);
                })
                .exceptionally(throwable -> {
                    // Unwrap CompletionException if needed
                    Throwable cause = throwable;
                    if (throwable instanceof CompletionException && throwable.getCause() != null) {
                        cause = throwable.getCause();
                    }

                    // Convert to SQLException using QueryExceptionHandler
                    String queryId = iterator.getQueryStatus() != null
                            ? iterator.getQueryStatus().getQueryId()
                            : null;

                    SQLException sqlException;
                    if (cause instanceof Exception) {
                        sqlException = QueryExceptionHandler.createException(
                                false, // includeCustomerDetailInReason
                                query,
                                queryId,
                                (Exception) cause);
                    } else {
                        // Wrap non-Exception throwables
                        sqlException = new SQLException("Query failed: " + cause.getMessage(), cause);
                    }

                    errorHolder.set(sqlException);
                    log.error("Query failed with SQLException", sqlException);
                    return null;
                });
    }
}
