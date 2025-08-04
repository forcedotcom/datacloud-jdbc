/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.common.collect.FluentIterable;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@AllArgsConstructor
public class StreamingByteStringChannel implements ReadableByteChannel {
    private final Iterator<ByteBuffer> iterator;
    private boolean open;
    private ByteBuffer currentBuffer;

    public static StreamingByteStringChannel of(Iterator<QueryResult> iterator) {
        val byteBuffers = FluentIterable.from(() -> iterator)
                .transform(StreamingByteStringChannel::fromQueryResult)
                .filter(StreamingByteStringChannel::isNotEmpty)
                .transform(ByteString::asReadOnlyByteBuffer) // Zero-copy!
                .iterator();

        return new StreamingByteStringChannel(
                byteBuffers,
                true,
                byteBuffers.hasNext() ? byteBuffers.next() : ByteString.EMPTY.asReadOnlyByteBuffer());
    }

    static ByteString fromQueryResult(QueryResult queryResult) {
        return Optional.ofNullable(queryResult)
                .map(QueryResult::getBinaryPart)
                .map(QueryResultPartBinary::getData)
                .orElse(ByteString.EMPTY);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }

        int totalBytesRead = 0;

        // Continue reading while destination has space AND we have data available
        while (dst.hasRemaining() && (currentBuffer.hasRemaining() || iterator.hasNext())) {
            if (!currentBuffer.hasRemaining()) {
                currentBuffer = iterator.next();
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

    static boolean isNotEmpty(ByteString buffer) {
        return !buffer.isEmpty();
    }
}
