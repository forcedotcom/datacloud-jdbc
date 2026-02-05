/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.NoSuchElementException;
import java.util.Optional;
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
        val blockingFuture = new CompletableFuture<Optional<String>>();
        val closeCalled = new AtomicBoolean(false);
        val iteratorStartedBlocking = new CountDownLatch(1);

        // Create an async iterator that blocks indefinitely until closed
        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Optional<String>> next() {
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
    void testNormalIteration() {
        val values = new String[] {"a", "b", "c"};
        val index = new AtomicInteger(0);

        AsyncIterator<String> asyncIterator = new AsyncIterator<String>() {
            @Override
            public CompletionStage<Optional<String>> next() {
                int i = index.getAndIncrement();
                if (i < values.length) {
                    return CompletableFuture.completedFuture(Optional.of(values[i]));
                }
                return CompletableFuture.completedFuture(Optional.empty());
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
            public CompletionStage<Optional<String>> next() {
                return CompletableFuture.completedFuture(Optional.empty());
            }

            @Override
            public void close() {}
        };

        val adapter = new SyncIteratorAdapter<>(asyncIterator);
        assertThat(adapter.hasNext()).isFalse();
    }
}
