/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryInfo;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamingByteStringChannel implements ReadableByteChannel {
    @AllArgsConstructor
    private static class ByteStringIterator<T> implements Iterator<Optional<ByteString>> {
        private final Iterator<T> inner;
        private final Function<T, QueryResultPartBinary> mapper;

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Optional<ByteString> next() {
            val next = inner.next();
            val mapped = mapper.apply(next);
            return Optional.ofNullable(mapped).map(QueryResultPartBinary::getData);
        }
    }

    public static StreamingByteStringChannel ofSchema(Iterator<QueryInfo> queryInfos) {
        val wrapped = new ByteStringIterator<QueryInfo>(queryInfos, QueryInfo::getBinarySchema);
        return new StreamingByteStringChannel(wrapped);
    }

    public static StreamingByteStringChannel ofResults(Iterator<QueryResult> queryResults) {
        val wrapped = new ByteStringIterator<QueryResult>(queryResults, QueryResult::getBinaryPart);
        return new StreamingByteStringChannel(wrapped);
    }

    public static StreamingByteStringChannel ofByteStrings(Iterator<ByteString> byteStrings) {
        val wrapped = new Iterator<Optional<ByteString>>() {
            @Override
            public boolean hasNext() {
                return byteStrings.hasNext();
            }

            @Override
            public Optional<ByteString> next() {
                return Optional.ofNullable(byteStrings.next());
            }
        };
        return new StreamingByteStringChannel(wrapped);
    }

    private final Iterator<Optional<ByteString>> iterator;
    private boolean open = true;
    private ByteBuffer currentBuffer = null;

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }

        int totalBytesRead = 0;

        // Continue reading while destination has space AND we have data available
        while (dst.hasRemaining() && (iterator.hasNext() || (currentBuffer != null && currentBuffer.hasRemaining()))) {
            if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                val data = iterator.next();
                if (data.isPresent() && !data.get().isEmpty()) {
                    currentBuffer = data.get().asReadOnlyByteBuffer();
                } else {
                    // This was a non result data query result message like a query info message.
                    // We ignore that message here and will just try to fetch the next message in
                    // the next loop iteration.
                    continue;
                }
            }

            val bytesTransferred = transferToDestination(currentBuffer, dst);
            totalBytesRead += bytesTransferred;

            // If no bytes were transferred, we can't make progress
            if (bytesTransferred == 0) {
                break;
            }
        }

        // Return -1 for end-of-stream if no bytes were read
        return totalBytesRead == 0 ? -1 : totalBytesRead;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        open = false;
    }

    private static int transferToDestination(ByteBuffer source, ByteBuffer destination) {
        if (source == null) {
            return 0;
        }

        int transfer = Math.min(destination.remaining(), source.remaining());
        if (transfer > 0) {
            val slice = source.slice();
            slice.limit(transfer);
            destination.put(slice);
            source.position(source.position() + transfer);
        }
        return transfer;
    }
}
