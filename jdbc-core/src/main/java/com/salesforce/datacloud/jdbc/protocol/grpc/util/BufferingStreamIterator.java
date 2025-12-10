/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc.util;

import io.grpc.stub.ClientResponseObserver;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;

/**
 * Iterator over a buffered gRPC streaming response.
 *
 * Uses a {@link BufferingStreamObserver} to decouple producer/consumer threads, request batches of
 * messages, and avoid lost messages when the consumer is slow. Presents standard {@link Iterator}
 * semantics and supports cancellation via {@link #close()}.
 *
 * Thread interruptions will cancel the underlying call and propagate as RuntimeExceptions.
 *
 * The class implements logic similar to {@link io.grpc.stub.ClientCalls.BlockingResponseStream},
 * the biggest difference is the unbounded buffering.
 *
 * @param <ReqT> the request message type
 * @param <RespT> the response message type
 */
public class BufferingStreamIterator<ReqT, RespT> implements Iterator<RespT> {
    private final BlockingQueue<StreamProgress<RespT>> queue;
    private final BufferingStreamObserver<ReqT, RespT> observer;
    private StreamProgress<RespT> next;
    private boolean done;

    public BufferingStreamIterator(String timingName, Logger logger) {
        this.queue = new LinkedBlockingQueue<>();
        this.observer = new BufferingStreamObserver<>(this.queue, timingName, logger);
    }

    public ClientResponseObserver<ReqT, RespT> getObserver() {
        return observer;
    }

    /**
     * Blocks until the next queued stream progress entry is available.
     * If interrupted, cancels the underlying gRPC call and re-sets the thread's interrupt flag.
     */
    private StreamProgress<RespT> waitForNext() {
        boolean interrupt = false;
        try {
            while (true) {
                try {
                    return queue.take();
                } catch (InterruptedException ie) {
                    interrupt = true;
                    // This will cause the ongoing call to be stopped and thus the queue will get an error element
                    // which will cause the while loop to exit.
                    try {
                        observer.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        } finally {
            if (interrupt) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Returns true if another response is available.
     * Fetches from the queue as needed and propagates upstream errors as RuntimeExceptions.
     */
    @Override
    public boolean hasNext() {
        while (next == null) {
            // We are always guaranteed to either get a success or failure message and thus this will never
            // block forever
            next = waitForNext();
        }

        // Either we have an error
        if (next.getError() != null) {
            Throwable t = next.getError();
            // Generally StatusRuntimeException would be the normal expected error type coming from gRPC framework
            throw (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
        } else {
            // Or we are completed (false) or have a value (true)
            return !next.isCompleted();
        }
    }

    @Override
    public RespT next() {
        if (!hasNext()) throw new NoSuchElementException();
        RespT v = next.getValue();
        next = null;
        return v;
    }

    /**
     * Closes the iterator and the underlying gRPC call, note that this can cancel server side processing so it
     * should only be used when server side processing should be stopped.
     */
    public void close() {
        observer.close();
    }
}
