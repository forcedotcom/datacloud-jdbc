/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.util;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * An asynchronous iterator that provides non-blocking iteration over elements.
 * Unlike traditional iterators, this uses {@link CompletionStage} to allow fully
 * asynchronous consumption without blocking threads.
 *
 * <p>The iteration pattern provides natural backpressure: the consumer controls
 * the pace by deciding when to request the next element.</p>
 *
 * <p>Usage pattern:</p>
 * <pre>{@code
 * AsyncIterator<T> iterator = ...;
 * iterator.next()
 *     .thenCompose(opt -> {
 *         if (opt.isPresent()) {
 *             process(opt.get());
 *             return iterator.next(); // continue iteration
 *         } else {
 *             return CompletableFuture.completedFuture(Optional.empty()); // done
 *         }
 *     });
 * }</pre>
 *
 * @param <T> the type of elements returned by this iterator
 */
public interface AsyncIterator<T> extends Closeable {

    /**
     * Returns a stage that completes with the next item,
     * or an empty Optional if the iteration is finished.
     *
     * <p>The returned stage may complete exceptionally if an error occurs
     * during iteration (e.g., network errors, protocol errors).</p>
     *
     * <p>Calling {@code next()} again before the previous stage completes
     * results in undefined behavior.</p>
     *
     * @return a CompletionStage that completes with the next element wrapped in Optional,
     *         or an empty Optional if no more elements are available
     */
    CompletionStage<Optional<T>> next();

    /**
     * Closes this iterator and releases any underlying resources.
     * This may cancel ongoing operations.
     *
     * <p>After closing, subsequent calls to {@link #next()} may return
     * completed stages with empty Optional or fail.</p>
     */
    @Override
    void close();
}
