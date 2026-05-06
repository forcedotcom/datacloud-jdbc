/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.junit.jupiter.api.Test;

class SyncIteratorAdapterTest {

    @Test
    void testInterruptHandlingRestoresInterruptFlag() throws Exception {
        val blockingFuture = new CompletableFuture<Step<String>>();
        val closeCalled = new AtomicBoolean(false);
        val iteratorStartedBlocking = new CountDownLatch(1);

        // Create an async iterator that blocks indefinitely until closed
        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Step<String>> next() {
                iteratorStartedBlocking.countDown();
                return blockingFuture;
            }

            @Override
            public void close() {
                closeCalled.set(true);
                // Simulate gRPC cancellation completing the future with error
                blockingFuture.completeExceptionally(new RuntimeException("Stream cancelled"));
            }
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);
        val threadInterrupted = new AtomicBoolean(false);
        val hasNextResult = new AtomicBoolean(true);

        // Run hasNext() in a separate thread and interrupt it
        Thread thread = new Thread(() -> {
            try {
                hasNextResult.set(adapter.hasNext());
            } catch (RuntimeException e) {
                // Expected - stream was cancelled
            }
            threadInterrupted.set(Thread.currentThread().isInterrupted());
        });

        thread.start();

        // Wait for the thread to start blocking on the future
        assertThat(iteratorStartedBlocking.await(5, TimeUnit.SECONDS)).isTrue();

        // Interrupt the thread
        thread.interrupt();

        // Wait for thread to finish
        thread.join(5000);
        assertThat(thread.isAlive()).isFalse();

        // Verify close was called due to interrupt
        assertThat(closeCalled.get()).isTrue();

        // Verify interrupt flag was restored
        assertThat(threadInterrupted.get()).isTrue();
    }

    @Test
    void testInterruptWithAsyncCloseSurfacesGrpcError() throws Exception {
        val blockingFuture = new CompletableFuture<Step<String>>();
        val closeCalled = new AtomicBoolean(false);
        val iteratorStartedBlocking = new CountDownLatch(1);

        // Simulate real gRPC behavior: close() does NOT synchronously complete the future.
        // The onError callback fires asynchronously after cancellation propagates through gRPC.
        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Step<String>> next() {
                iteratorStartedBlocking.countDown();
                return blockingFuture;
            }

            @Override
            public void close() {
                closeCalled.set(true);
                // Simulate async gRPC onError: complete the future on a different thread after a delay
                new Thread(() -> {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            blockingFuture.completeExceptionally(
                                    new RuntimeException("CANCELLED: call closed by client"));
                        })
                        .start();
            }
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);
        val thrownException = new java.util.concurrent.atomic.AtomicReference<Throwable>();

        Thread thread = new Thread(() -> {
            try {
                adapter.hasNext();
            } catch (Throwable t) {
                thrownException.set(t);
            }
        });

        thread.start();
        assertThat(iteratorStartedBlocking.await(5, TimeUnit.SECONDS)).isTrue();

        thread.interrupt();
        thread.join(5000);
        assertThat(thread.isAlive()).isFalse();

        // Must not throw IllegalStateException("Unfulfilled previous future when next is requested")
        // Instead, the gRPC cancellation error should surface
        assertThat(closeCalled.get()).isTrue();
        assertThat(thrownException.get())
                .isNotNull()
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("CANCELLED");
    }

    @Test
    void testHasNextAfterErrorRethrowsWithoutRequestingNewFuture() {
        val nextCallCount = new AtomicInteger(0);
        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Step<String>> next() {
                nextCallCount.incrementAndGet();
                val failed = new CompletableFuture<Step<String>>();
                failed.completeExceptionally(new RuntimeException("stream error"));
                return failed;
            }

            @Override
            public void close() {}
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);

        assertThatThrownBy(adapter::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("stream error");

        // Subsequent calls must re-surface the same error without re-driving the iterator. This
        // preserves the original error for callers that probe the stream twice (e.g. execute()
        // followed by getResultSet()) instead of masking it as an empty stream.
        assertThatThrownBy(adapter::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("stream error");
        assertThat(nextCallCount.get()).isEqualTo(1);
    }

    @Test
    void testNormalIteration() {
        val values = new String[] {"a", "b", "c"};
        val index = new AtomicInteger(0);

        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Step<String>> next() {
                int i = index.getAndIncrement();
                if (i < values.length) {
                    return CompletableFuture.completedFuture(Step.value(values[i]));
                }
                return CompletableFuture.completedFuture(Step.<String>done());
            }

            @Override
            public void close() {}
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);

        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo("a");
        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo("b");
        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo("c");
        assertThat(adapter.hasNext()).isFalse();
        // Check that repeated calls stay false
        assertThat(adapter.hasNext()).isFalse();
        // Check that next() throws an exception
        assertThatThrownBy(adapter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testEmptyIterator() {
        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Step<String>> next() {
                return CompletableFuture.completedFuture(Step.<String>done());
            }

            @Override
            public void close() {}
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);
        assertThat(adapter.hasNext()).isFalse();
    }

    @Test
    void testNeedDispatchRunsThunkOnPumpThreadAndContinues() {
        val dispatchedOnThread = new java.util.concurrent.atomic.AtomicReference<String>();
        val dispatchedCount = new AtomicInteger(0);
        val callCount = new AtomicInteger(0);

        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Step<String>> next() {
                int call = callCount.getAndIncrement();
                switch (call) {
                    case 0:
                        // First call: ask pump to dispatch a thunk on its own thread
                        return CompletableFuture.completedFuture(Step.<String>needDispatch(() -> {
                            dispatchedOnThread.set(Thread.currentThread().getName());
                            dispatchedCount.incrementAndGet();
                        }));
                    case 1:
                        return CompletableFuture.completedFuture(Step.value("only"));
                    default:
                        return CompletableFuture.completedFuture(Step.<String>done());
                }
            }

            @Override
            public void close() {}
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);
        val callerThread = Thread.currentThread().getName();

        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo("only");
        assertThat(adapter.hasNext()).isFalse();

        // The dispatch thunk must have run on the same thread that called hasNext().
        assertThat(dispatchedCount.get()).isEqualTo(1);
        assertThat(dispatchedOnThread.get()).isEqualTo(callerThread);
    }
}
