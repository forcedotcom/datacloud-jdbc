/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Adapter that wraps an {@link AsyncIterator} to provide a synchronous {@link Iterator} interface.
 *
 * <p>This adapter blocks the calling thread when waiting for async operations to complete.
 * It is intended as a compatibility layer for existing synchronous code that cannot be
 * easily migrated to async patterns.</p>
 *
 * <p>Thread interruptions during blocking operations will close the underlying async iterator
 * and re-set the thread's interrupt flag.</p>
 *
 * @param <T> the type of elements returned by this iterator
 */
public class SyncIteratorAdapter<T> implements Iterator<T>, AutoCloseable {

    /** The underlying async iterator being wrapped. */
    private final AsyncIterator<T> asyncIterator;
    /** The prefetched next value, or null if not yet fetched. Empty Optional signals end of iteration. */
    private Optional<T> nextValue;
    /** Whether iteration has completed (either naturally or due to interruption). */
    private boolean done;

    /**
     * Creates a new sync adapter wrapping the given async iterator.
     *
     * @param asyncIterator the async iterator to wrap
     */
    public SyncIteratorAdapter(AsyncIterator<T> asyncIterator) {
        this.asyncIterator = asyncIterator;
        this.nextValue = null;
        this.done = false;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method blocks until the next element is available or the stream ends.
     * If the thread is interrupted while waiting, the underlying async iterator is closed
     * and the thread's interrupt flag is restored.</p>
     *
     * @throws RuntimeException if the underlying async operation fails
     */
    @Override
    // TODO: Deeply vet this
    public boolean hasNext() {
        if (done) {
            return false;
        }
        if (nextValue != null) {
            return nextValue.isPresent();
        }

        // Block waiting for next value
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    nextValue = asyncIterator.next().toCompletableFuture().get();
                    if (!nextValue.isPresent()) {
                        done = true;
                    }
                    return nextValue.isPresent();
                } catch (InterruptedException ie) {
                    interrupted = true;
                    // Close the async iterator on interruption
                    try {
                        asyncIterator.close();
                    } catch (Exception ignore) {
                    }
                    done = true;
                    return false;
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                } catch (CompletionException ce) {
                    Throwable cause = ce.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the next element, blocking if necessary via {@link #hasNext()}.</p>
     *
     * @throws NoSuchElementException if no more elements are available
     * @throws RuntimeException if the underlying async operation fails
     */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T value = nextValue.get();
        nextValue = null;
        return value;
    }

    /**
     * Closes this adapter and the underlying async iterator.
     *
     * <p>This may cancel any pending async operations.</p>
     */
    @Override
    public void close() {
        asyncIterator.close();
    }
}
