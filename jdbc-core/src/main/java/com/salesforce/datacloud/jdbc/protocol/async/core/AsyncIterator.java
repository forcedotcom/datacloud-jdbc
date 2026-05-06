/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.core;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;

/**
 * An asynchronous iterator that provides non-blocking iteration over elements.
 * Unlike traditional iterators, this uses {@link CompletionStage} to allow fully
 * asynchronous consumption without blocking threads.
 *
 * <p>The iteration pattern provides natural backpressure: the consumer controls
 * the pace by deciding when to request the next element.</p>
 *
 * <p>Each call to {@link #next()} produces a {@link Step}: a value, end-of-stream, or a
 * {@link Step.NeedDispatch} that asks the synchronous consumer to run a side-effecting thunk on
 * its own thread before requesting again. {@code NeedDispatch} is how follow-up gRPC calls keep
 * firing on the caller's thread so {@link io.grpc.ClientInterceptor} {@code start} callbacks see
 * caller-thread {@link ThreadLocal}s.</p>
 *
 * <p>Usage pattern:</p>
 * <pre>{@code
 * AsyncIterator<T> iterator = ...;
 * iterator.next()
 *     .thenCompose(step -> {
 *         if (step instanceof Step.Value) {
 *             process(((Step.Value<T>) step).getItem());
 *             return iterator.next(); // continue iteration
 *         } else if (step instanceof Step.NeedDispatch) {
 *             ((Step.NeedDispatch<T>) step).getDispatch().run();
 *             return iterator.next();
 *         } else {
 *             return CompletableFuture.completedFuture(step); // Done
 *         }
 *     });
 * }</pre>
 *
 * @param <T> the type of elements returned by this iterator
 */
public interface AsyncIterator<T> extends Closeable {

    /**
     * Returns a stage that completes with the next {@link Step}.
     *
     * <p>The returned stage may complete exceptionally if an error occurs
     * during iteration (e.g., network errors, protocol errors).</p>
     *
     * <p>Calling {@code next()} again before the previous stage completes
     * results in undefined behavior.</p>
     *
     * @return a CompletionStage that completes with the next {@link Step}
     */
    CompletionStage<Step<T>> next();

    /**
     * Closes this iterator and releases any underlying resources.
     * This may cancel ongoing operations.
     *
     * <p>After closing, subsequent calls to {@link #next()} may return
     * completed stages with {@link Step#done()} or fail.</p>
     */
    @Override
    void close();
}
