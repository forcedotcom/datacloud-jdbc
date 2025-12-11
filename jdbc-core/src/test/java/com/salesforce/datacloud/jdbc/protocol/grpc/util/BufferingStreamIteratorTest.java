/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.grpc.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.InProcessGrpcMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryInfoParam;

@Slf4j
@ExtendWith(InProcessGrpcMockExtension.class)
class BufferingStreamIteratorTest {

    @Test
    @SneakyThrows
    void bufferingStreamIterator_interruptHasNextCancelsAndPreservesFlag() {
        val channel = InProcessChannelBuilder.forName(GrpcMock.getGlobalInProcessName())
                .usePlaintext()
                .build();
        val stub = HyperServiceGrpc.newStub(channel);

        // Setup a blocking query info call that - if the latch is not release - would block for 60 seconds
        val blockLatch = new CountDownLatch(1);
        GrpcMock.stubFor(GrpcMock.serverStreamingMethod(HyperServiceGrpc.getGetQueryInfoMethod())
                .willProxyTo((request, observer) -> {
                    try {
                        blockLatch.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                    }
                }));

        // Setup a call that uninterrupted would take 60s on a separate thread
        val iterator = new BufferingStreamIterator<QueryInfoParam, QueryInfo>("test-buffering-iterator", log);
        val request = QueryInfoParam.newBuilder().setQueryId("qid").build();
        stub.getQueryInfo(request, iterator.getObserver());
        val thrown = new AtomicReference<Throwable>();
        val interrupted = new AtomicBoolean(false);
        val t = new Thread(() -> {
            try {
                iterator.hasNext();
            } catch (Throwable ex) {
                thrown.set(ex);
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        t.start();

        // Wait till the thread has picked up the iterator and then interrupt
        Thread.sleep(150);
        t.interrupt();
        t.join(5_000);

        assertThat(thrown.get())
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("CANCELLED: Call got closed by the client.");
        assertThat(interrupted.get()).isTrue();

        // Only after we have validated that the iterator returns and the expected exception was thrown lets unlatch as
        // it shouldn't be required
        // for returning
        blockLatch.countDown();
    }
}
