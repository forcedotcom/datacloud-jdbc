/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.core;

import com.salesforce.datacloud.jdbc.protocol.CloseableIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Adapter that wraps an {@link AsyncIterator} to provide a synchronous {@link Iterator} interface.
 *
 * <p>This adapter blocks the calling thread when waiting for async operations to complete.
 * It is intended as a compatibility layer for existing synchronous code that cannot be
 * easily migrated to async patterns.</p>
 *
 * <p>When the underlying async iterator returns a {@link Step.NeedDispatch}, the dispatch thunk
 * is run on the caller thread (the one currently blocked inside {@link #hasNext()}). This is how
 * follow-up gRPC calls are kept on the caller's thread so {@link io.grpc.ClientInterceptor}
 * {@code start} callbacks observe caller-thread {@link ThreadLocal}s.</p>
 *
 * <p>Thread interruptions during blocking operations will close the underlying async iterator
 * and re-set the thread's interrupt flag.</p>
 *
 * @param <T> the type of elements returned by this iterator
 */
public class SyncIteratorAdapter<T> implements CloseableIterator<T> {

    /** The underlying async iterator being wrapped. */
    private final AsyncIterator<T> asyncIterator;
    /** The prefetched next value, or null if not yet fetched. Empty Optional signals end of iteration. */
    private Optional<T> nextValue;
    /** Whether iteration has completed (either naturally or due to interruption). */
    private boolean done;
    /** Terminal error from a previous call, re-thrown on subsequent invocations. */
    private RuntimeException terminalError;

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
     * <p>If the async iterator returns a {@link Step.NeedDispatch}, its dispatch thunk runs on
     * this (caller) thread and iteration continues until a value or end-of-stream is observed.</p>
     *
     * @throws RuntimeException if the underlying async operation fails
     */
    @Override
    public boolean hasNext() {
        if (terminalError != null) {
            throw terminalError;
        }
        if (done) {
            return false;
        }
        if (nextValue != null) {
            return nextValue.isPresent();
        }

        // Outer pump: each iteration either gets a value/done, or runs a dispatch thunk and tries again.
        while (true) {
            // Block waiting for the next step. The future is hoisted out of the inner loop so that on
            // interrupt we close the iterator (triggering gRPC cancellation) and then re-wait on the
            // *same* future rather than requesting a new one (which would hit "Unfulfilled previous future").
            boolean interrupted = false;
            CompletableFuture<Step<T>> future = asyncIterator.next().toCompletableFuture();
            Step<T> step;
            try {
                while (true) {
                    try {
                        step = future.get();
                        break;
                    } catch (InterruptedException ie) {
                        interrupted = true;
                        try {
                            asyncIterator.close();
                        } catch (Exception ignore) {
                        }
                    } catch (ExecutionException | CompletionException ee) {
                        // The async stream is permanently dead after an error. Cache the exception so a
                        // retried hasNext() re-surfaces it instead of requesting a new future (which
                        // would hit "Unfulfilled previous future" or mask the original error).
                        Throwable cause = ee.getCause();
                        terminalError = (cause instanceof RuntimeException)
                                ? (RuntimeException) cause
                                : new RuntimeException(cause);
                        throw terminalError;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            if (step instanceof Step.NeedDispatch) {
                // Run the dispatch thunk on this (caller) thread so any gRPC ClientInterceptor.start
                // callbacks fire here and observe caller-thread ThreadLocals. Then re-pump.
                //
                // If the dispatch thunk throws (e.g. a host interceptor's start() rejects the call),
                // cache the exception so subsequent hasNext() calls re-surface it rather than
                // re-driving the iterator into an unspecified state.
                try {
                    ((Step.NeedDispatch<T>) step).getDispatch().run();
                } catch (RuntimeException re) {
                    terminalError = re;
                    throw terminalError;
                }
                continue;
            } else if (step instanceof Step.Value) {
                T item = ((Step.Value<T>) step).getItem();
                nextValue = Optional.of(item);
                return true;
            } else if (step instanceof Step.Done) {
                nextValue = Optional.empty();
                done = true;
                return false;
            }
            throw new IllegalStateException("Unknown Step subtype: " + step.getClass());
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
