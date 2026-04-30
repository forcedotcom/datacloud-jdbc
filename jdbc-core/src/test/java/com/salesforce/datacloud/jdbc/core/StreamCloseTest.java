/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperServerManager;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.protocol.CloseableIterator;
import com.salesforce.datacloud.jdbc.protocol.QueryResultIterator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryParam;

/**
 * Tests that closing a ResultSet properly cancels the underlying gRPC streams.
 *
 * <p>This is a regression test for the stream leak bug where {@link ByteStringReadableByteChannel}
 * did not propagate close to the underlying gRPC iterator, causing channels to hang on
 * graceful shutdown with gRPC 1.80+.</p>
 *
 * <p>The driver uses {@code shutdownNow()} in {@link JdbcDriverStubProvider#close()} as defense-in-depth
 * to ensure Connection.close() never hangs, even if some close paths are missed.</p>
 */
@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class StreamCloseTest {

    /**
     * Verifies that closing a StreamingResultSet triggers close on the underlying gRPC iterator
     * through the full Arrow close chain:
     * StreamingResultSet.close() → ArrowStreamReaderCursor.close() → ArrowStreamReader.close()
     * → ArrowReader.closeReadSource() → MessageChannelReader → ReadChannel
     * → ByteStringReadableByteChannel.close() → SQLExceptionQueryResultIterator.close()
     * → QueryResultIterator.close() → AsyncStreamObserver.close() → gRPC stream cancel.
     *
     * <p>The test wraps a QueryResultIterator in a close-tracking decorator, passes it through the
     * standard driver path (SQLExceptionQueryResultIterator → QueryResultArrowStream →
     * ByteStringReadableByteChannel → ArrowStreamReader → StreamingResultSet), then verifies that
     * closing the ResultSet propagates all the way down to the iterator.</p>
     */
    @Test
    @SneakyThrows
    void closingResultSetClosesUnderlyingIterator() {
        val server = HyperServerManager.get(HyperServerManager.ConfigFile.SMALL_CHUNKS);

        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();

        try {
            val stub = HyperServiceGrpc.newStub(channel);
            val queryParam = QueryParam.newBuilder()
                    .setSql("select a from generate_series(1, 10000) as s(a)")
                    .setTransferMode(QueryParam.TransferMode.ADAPTIVE)
                    .setOutputFormat(OutputFormat.ARROW_IPC)
                    .build();

            // Create the low-level gRPC stream iterator
            val iterator = QueryResultIterator.of(stub, queryParam);
            assertThat(iterator.hasNext()).isTrue();

            // Wrap the iterator in a close-tracking decorator that goes through the standard path
            val closeCalled = new AtomicBoolean(false);
            val tracked = new CloseTrackingIterator(iterator, closeCalled);

            // Build the ArrowStreamReader through the standard driver path.
            // This exercises: SQLExceptionQueryResultIterator → QueryResultArrowStream →
            // ByteStringReadableByteChannel(iterator, resource) → ArrowStreamReader
            val arrowStream = SQLExceptionQueryResultIterator.createSqlExceptionArrowStreamReader(
                    tracked, false, "test-query", null);
            val resultSet = StreamingResultSet.of(arrowStream, "test-query");

            // Read one row — stream is still open with remaining rows
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);

            // Verify close has NOT been called yet
            assertThat(closeCalled.get()).isFalse();

            // Close the ResultSet — this must propagate through the full Arrow close chain
            resultSet.close();

            assertThat(closeCalled.get())
                    .as("ResultSet.close() must propagate through the Arrow close chain to close "
                            + "the underlying gRPC iterator via ByteStringReadableByteChannel. "
                            + "If this fails, gRPC streams will leak and channel.shutdown() will hang.")
                    .isTrue();
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A CloseableIterator wrapper that delegates to an underlying iterator and tracks
     * whether close() was called. Used to verify the close chain propagates through
     * ByteStringReadableByteChannel.
     */
    private static class CloseTrackingIterator implements CloseableIterator<salesforce.cdp.hyperdb.v1.QueryResult> {
        private final QueryResultIterator delegate;
        private final AtomicBoolean closeCalled;

        CloseTrackingIterator(QueryResultIterator delegate, AtomicBoolean closeCalled) {
            this.delegate = delegate;
            this.closeCalled = closeCalled;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public salesforce.cdp.hyperdb.v1.QueryResult next() {
            return delegate.next();
        }

        @Override
        public void close() {
            closeCalled.set(true);
            delegate.close();
        }
    }
}
