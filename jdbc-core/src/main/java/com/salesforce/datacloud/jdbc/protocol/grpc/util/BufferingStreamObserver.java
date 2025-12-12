/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc.util;

import com.google.protobuf.AbstractMessage;
import com.salesforce.datacloud.jdbc.logging.ElapsedLogger;
import java.util.Queue;
import lombok.NonNull;
import org.slf4j.Logger;

/**
 * An observer that decouples gRPC callbacks from the target consumer by enqueueing them as
 * {@link StreamProgress} entries into a {@link java.util.Queue} for ordered, asynchronous execution.
 * Goals:
 * - Prevent unnecessary call cancellation due to slow consumers by decoupling producer/consumer threads.
 * - Data integrity: reduces risk of missed messages that could otherwise cause data loss for
 *   downstream consumers (e.g., the JDBC driver) if a call is cancelled while the consumer lags.
 * Cancellation semantics:
 * - When the observer is closed via its parent closable contract, the underlying gRPC call is cancelled.
 * - Some callbacks may still be processed if they were already enqueued before cancellation.
 * Event semantics:
 * - {@link #onNext(Object)} enqueues {@link StreamProgress#success(Object)}.
 * - {@link #onError(Throwable)} enqueues {@link StreamProgress#failure(Throwable)} (and logs failure timing).
 * - {@link #onCompleted()} enqueues {@link StreamProgress#completed()} (and logs success timing).
 *
 * @param <ReqT> The request type
 * @param <RespT> The response type
 */
public class BufferingStreamObserver<ReqT, RespT extends AbstractMessage>
        extends ClosableCancellingStreamObserver<ReqT, RespT> {
    private final Queue<StreamProgress<RespT>> queue;
    private final Logger logger;
    private final String timingName;
    private final long startNanos;
    private long totalResponseSize = 0;

    /**
     * Create a buffering observer with optional timing/logging.
     * If {@code logger} and {@code timingName} are provided, a start, success, or failure entry
     * will be logged mirroring {@link com.salesforce.datacloud.jdbc.logging.ElapsedLogger}'s format.
     *
     * @param queue       the queue receiving {@link StreamProgress} events (value, error, completed) in order.
     *                    It must not fail on offer.
     * @param timingName  an identifier for logging
     * @param logger      the logger to write timing to
     */
    public BufferingStreamObserver(
            @NonNull Queue<StreamProgress<RespT>> queue, @NonNull String timingName, @NonNull Logger logger) {
        this.queue = queue;
        this.logger = logger;
        this.timingName = timingName;
        this.startNanos = System.nanoTime();
        ElapsedLogger.logStart(this.logger, this.timingName);
    }

    @Override
    public void onNext(RespT value) {
        totalResponseSize += value.getSerializedSize();
        queue.offer(StreamProgress.success(value));
        // We have to manually request the next message as ClosableCancellingStreamObserver has manual flow control.
        callStream.request(1);
    }

    @Override
    public void onError(Throwable t) {
        long elapsed = System.nanoTime() - startNanos;
        ElapsedLogger.logFailure(
                logger, timingName + ", responseSizeMb=" + totalResponseSize / 1_000_000.0, elapsed, t);
        queue.offer(StreamProgress.failure(t));
    }

    @Override
    public void onCompleted() {
        long elapsed = System.nanoTime() - startNanos;
        ElapsedLogger.logSuccess(logger, timingName + ", responseSizeMb=" + totalResponseSize / 1_000_000.0, elapsed);
        queue.offer(StreamProgress.completed());
    }
}
