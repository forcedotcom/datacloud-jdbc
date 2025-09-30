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
import lombok.NonNull;

/**
 * A ReadableByteChannel that exposes an Iterator<ByteString> as a stream of bytes.
 * This class has a single responsibility: converting ByteString iterator to byte stream.
 */
public class ByteStringReadableByteChannel implements ReadableByteChannel {
    @NonNull private final Iterator<ByteString> iterator;

    private boolean open = true;
    private ByteBuffer currentBuffer = null;

    public ByteStringReadableByteChannel(@NonNull Iterator<ByteString> iterator) {
        this.iterator = iterator;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }

        int totalBytesRead = 0;

        // Continue reading while destination has space AND we have data available
        while (dst.hasRemaining() && (iterator.hasNext() || (currentBuffer != null && currentBuffer.hasRemaining()))) {
            if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                if (iterator.hasNext()) {
                    ByteString data = iterator.next();
                    currentBuffer = data.asReadOnlyByteBuffer();
                } else {
                    break; // No more data
                }
            }

            int bytesTransferred = transferToDestination(currentBuffer, dst);
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
        int transfer = Math.min(destination.remaining(), source.remaining());
        if (transfer > 0) {
            ByteBuffer slice = source.slice();
            slice.limit(transfer);
            destination.put(slice);
            source.position(source.position() + transfer);
        }
        return transfer;
    }
}
