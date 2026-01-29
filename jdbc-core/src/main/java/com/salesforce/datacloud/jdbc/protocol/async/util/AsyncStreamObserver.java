/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.util;

import com.google.protobuf.AbstractMessage;
import com.salesforce.datacloud.jdbc.logging.ElapsedLogger;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.NonNull;
import org.slf4j.Logger;

/**
 * An asynchronous observer for gRPC streaming responses that uses {@link CompletableFuture}
 * instead of blocking queues. This observer buffers incoming messages and allows
 * asynchronous consumption via {@link #requestNext()}.
 *
 * <p>The observer prefetches messages from gRPC to avoid data loss when the consumer
 * is slow (as the V3 Protocol treats cancellation sometimes as expected outcome but then
 * it would be ambiguous whether all messages have been received). Messages are buffered
 * in memory until consumed.</p>
 *
 * <p>Thread-safety: All state mutations are protected by synchronization on {@code this}.
 * The observer can be closed from any thread.</p>
 *
 * @param <ReqT>  the request message type
 * @param <RespT> the response message type
 */
public class AsyncStreamObserver<ReqT, RespT extends AbstractMessage> implements ClientResponseObserver<ReqT, RespT> {
    // Initial number of messages to request from gRPC, given that Hyper targets 16mb per stream and 1mb messages this
    // should
    // be sufficient to request all in one go (and it's also sufficiently large to hide latency on slow connections).
    // Larger streams are still supported and more messages will get requested as messages come in to aim for always
    // INITIAL_REQUEST_COUNT outstanding messages.
    private static final int INITIAL_REQUEST_COUNT = 16;

    // The logger which should be used for logging
    private final Logger logger;
    // The name to use for elapse time logging
    private final String timingName;
    // Used for elapsed time logging
    private final long startNanos;
    // The total size of the response in bytes used for logging
    private long totalResponseSize = 0;

    // The call stream for flow control and cancellation
    private volatile ClientCallStreamObserver<ReqT> callStream;
    // Tracks whether close was requested, is required when the close comes before the stream is properly
    // started.
    private volatile boolean closeRequested = false;
    // Tracks whether the stream has completed (onCompleted or onError called)
    private volatile boolean streamEnded = false;
    // The terminal error, if any
    private volatile Throwable terminalError = null;

    // Buffer for incoming messages
    private final Queue<RespT> buffer = new ArrayDeque<>();
    // The pending future that will be completed when the next message arrives (if buffer is empty)
    private CompletableFuture<Optional<RespT>> pendingFuture = null;

    /**
     * Creates an async stream observer with timing/logging support.
     *
     * @param timingName an identifier for logging
     * @param logger     the logger to write timing to
     */
    public AsyncStreamObserver(@NonNull String timingName, @NonNull Logger logger) {
        this.logger = logger;
        this.timingName = timingName;
        this.startNanos = System.nanoTime();
        ElapsedLogger.logStart(this.logger, this.timingName);
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<ReqT> callStream) {
        this.callStream = callStream;
        // Request initial batch of messages to start buffering
        callStream.disableAutoRequestWithInitial(INITIAL_REQUEST_COUNT);
        if (closeRequested) {
            callStream.cancel("Call got closed by the client.", null);
        }
    }

    @Override
    public synchronized void onNext(RespT value) {
        totalResponseSize += value.getSerializedSize();

        // If there's a pending future waiting for data, complete it directly
        if (pendingFuture != null) {
            CompletableFuture<Optional<RespT>> future = pendingFuture;
            pendingFuture = null;
            future.complete(Optional.of(value));
        } else {
            // Otherwise buffer the message
            buffer.add(value);
        }

        // Request the next message to keep the pipeline flowing
        if (callStream != null) {
            callStream.request(1);
        }
    }

    @Override
    public synchronized void onError(Throwable t) {
        long elapsed = System.nanoTime() - startNanos;
        ElapsedLogger.logFailure(
                logger, timingName + ", responseSizeMb=" + totalResponseSize / 1_000_000.0, elapsed, t);
        streamEnded = true;
        terminalError = t;

        // Complete any pending future with the error
        if (pendingFuture != null) {
            CompletableFuture<Optional<RespT>> future = pendingFuture;
            pendingFuture = null;
            future.completeExceptionally(t);
        }
    }

    @Override
    public synchronized void onCompleted() {
        long elapsed = System.nanoTime() - startNanos;
        ElapsedLogger.logSuccess(logger, timingName + ", responseSizeMb=" + totalResponseSize / 1_000_000.0, elapsed);
        streamEnded = true;

        // Complete any pending future with empty (only if buffer is also empty)
        if (pendingFuture != null && buffer.isEmpty()) {
            CompletableFuture<Optional<RespT>> future = pendingFuture;
            pendingFuture = null;
            future.complete(Optional.empty());
        }
    }

    /**
     * Requests the next element from the stream asynchronously.
     *
     * <p>Returns a CompletionStage that will complete with:</p>
     * <ul>
     *   <li>Optional containing the next value when available</li>
     *   <li>Empty Optional when the stream is complete</li>
     *   <li>Exceptionally if an error occurred</li>
     * </ul>
     *
     *
     * @return a CompletionStage for the next element
     */
    public synchronized CompletionStage<Optional<RespT>> requestNext() {
        // If there are buffered messages, return one immediately
        if (!buffer.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.of(buffer.poll()));
        }

        // If stream already ended, return immediately
        if (streamEnded) {
            if (terminalError != null) {
                CompletableFuture<Optional<RespT>> future = new CompletableFuture<>();
                future.completeExceptionally(terminalError);
                return future;
            }
            // Empty signals success
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // Create a new future that will be completed when data arrives
        // Fail if there is an unconsumed future (would indicate concurrent requestNext calls
        // which are not supported)
        if (pendingFuture != null) {
            throw new IllegalStateException("Unconsumed future");
        }
        pendingFuture = new CompletableFuture<>();
        return pendingFuture;
    }

    /**
     * Closes the observer and cancels the underlying gRPC call.
     * After closing, subsequent calls to {@link #requestNext()} will return empty Optionals.
     */
    public void close() {
        closeRequested = true;
        ClientCallStreamObserver<ReqT> stream = callStream;
        if (stream != null) {
            stream.cancel("Call got closed by the client.", null);
        }
    }
}
