/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc.util;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.Closeable;

/**
 * Extension of {@link ClientResponseObserver} that is {@link Closeable}. It provides a
 * robust way to cancel an ongoing or subsequently created gRPC call, ensuring resources are
 * cleaned up and the stream is closed. This class uses manual flow control, so implementors
 * will have to make sure to always call request when they receive a value from onNext.
 *
 * Semantics:
 * - Calling {@link #close()} marks the observer as closed and cancels the current call if present.
 * - If a new call is started after {@link #close()} (e.g., due to retry logic), it will be
 *   immediately cancelled in {@link #beforeStart(ClientCallStreamObserver)}.
 *
 * Thread-safety:
 * - Uses a volatile flag to detect prior close requests across threads and races between
 *   close and call creation.
 *
 * Note: After cancellation, gRPC may still invoke terminal callbacks (e.g., onError) as per
 * the gRPC contract.
 * @param <ReqT> The request type
 * @param <RespT> The response type
 */
public abstract class ClosableCancellingStreamObserver<ReqT, RespT> implements ClientResponseObserver<ReqT, RespT> {
    // The call stream observer that is used to cancel the call and potentially manage flow control.
    protected ClientCallStreamObserver<ReqT> callStream;
    // Tracks whether a close was requested before the call stream was available.
    private volatile boolean closeRequested = false;

    /**
     * Receives the {@link ClientCallStreamObserver} before the call starts. If a prior
     * {@link #close()} was requested, the call is cancelled immediately to avoid starting
     * a new stream after shutdown.
     *
     * @param callStream the call stream observer provided by gRPC
     */
    @Override
    public void beforeStart(ClientCallStreamObserver<ReqT> callStream) {
        this.callStream = callStream;
        // Request many messages to avoid backpressure, with 16 outstanding requests we should have enough buffering
        // so that the consumers have time to put it into their buffer.
        callStream.disableAutoRequestWithInitial(16);
        if (closeRequested) {
            // If close was requested before the call started, immediately cancel this stream.
            callStream.cancel("Call got closed by the client.", null);
        }
    }

    /**
     * Marks this observer as closed and cancels the current call if present. Any subsequent
     * call that starts will be immediately cancelled in {@link #beforeStart(ClientCallStreamObserver)}.
     * Note that this can cancel server side processing so it should only be used when server side
     * processing should be stopped.
     */
    public void close() {
        closeRequested = true;
        if (callStream != null) {
            callStream.cancel("Call got closed by the client.", null);
            // After this the ClientCallStreamObserver will not process any further messages and the server is informed
            // of the cancellation.
            // The ClientResponseObserver methods will still get called with at least the onError method.
        }
    }
}
