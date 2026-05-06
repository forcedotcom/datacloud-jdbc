/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.core;

import com.google.protobuf.AbstractMessage;
import io.grpc.stub.ClientResponseObserver;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;

/**
 * Asynchronous iterator over a gRPC response.
 *
 * <p>This iterator only consumes an already-running gRPC stream — it never initiates an RPC
 * itself, so it never emits {@link Step.NeedDispatch}. Each call to {@link #next()} produces
 * either {@link Step.Value} (next message), {@link Step.Done} (stream completed), or completes
 * the stage exceptionally on stream error.</p>
 *
 * @param <ReqT>  the request message type
 * @param <RespT> the response message type
 */
public class AsyncStreamObserverIterator<ReqT, RespT extends AbstractMessage> implements AsyncIterator<RespT> {

    // The observer that handles the interaction at the gRPC level
    private final AsyncStreamObserver<ReqT, RespT> observer;

    /**
     * Creates a new async buffering stream iterator.
     *
     * @param timingName an identifier for logging
     * @param logger     the logger to write timing to
     */
    public AsyncStreamObserverIterator(String timingName, Logger logger) {
        this.observer = new AsyncStreamObserver<>(timingName, logger);
    }

    /**
     * Returns the observer for use with gRPC stub methods.
     *
     * @return the observer to pass to gRPC streaming calls
     */
    public ClientResponseObserver<ReqT, RespT> getObserver() {
        return observer;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Requests the next message from the gRPC stream. The returned stage completes
     * with {@link Step.Value} when a message is received, {@link Step.Done} when the stream ends,
     * or exceptionally on stream error.</p>
     */
    @Override
    public CompletionStage<Step<RespT>> next() {
        return observer.requestNext().thenApply(opt -> opt.isPresent() ? Step.value(opt.get()) : Step.<RespT>done());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Closes the iterator and cancels the underlying gRPC call. This can cancel
     * server-side processing, so it should only be used when server-side processing
     * should be stopped.</p>
     */
    @Override
    public void close() {
        observer.close();
    }
}
