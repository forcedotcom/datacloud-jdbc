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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import lombok.RequiredArgsConstructor;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;

@RequiredArgsConstructor
public class StreamingByteStringChannel implements ReadableByteChannel {
    private final Iterator<QueryResult> iterator;
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
                val queryResult = iterator.next();
                if (queryResult.hasBinaryPart()) {
                    val data = queryResult.getBinaryPart().getData();
                    if (!data.isEmpty()) {
                        currentBuffer = data.asReadOnlyByteBuffer();
                    }
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
