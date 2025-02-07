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
import com.salesforce.datacloud.jdbc.util.ConsumingPeekingIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryResult;
import salesforce.cdp.hyperdb.v1.QueryResultPartBinary;

@Slf4j
public class ExecuteQueryResponseChannel implements ReadableByteChannel {
    private static final ByteBuffer empty = ByteBuffer.allocateDirect(0);
    private final Iterator<ByteBuffer> iterator;

    public static ExecuteQueryResponseChannel of(Stream<QueryResult> stream) {
        return new ExecuteQueryResponseChannel(stream.map(ExecuteQueryResponseChannel::fromQueryResult));
    }

    private ExecuteQueryResponseChannel(Stream<ByteBuffer> stream) {
        this.iterator = ConsumingPeekingIterator.of(stream, ExecuteQueryResponseChannel::isNotEmpty);
    }

    static ByteBuffer fromQueryResult(QueryResult queryResult) {
        return Optional.ofNullable(queryResult)
                .map(QueryResult::getBinaryPart)
                .map(QueryResultPartBinary::getData)
                .map(ByteString::toByteArray)
                .map(ByteBuffer::wrap)
                .orElse(empty);
    }

    @Override
    public int read(ByteBuffer destination) {
        if (this.iterator.hasNext()) {
            return transferToDestination(iterator.next(), destination);
        } else {
            return -1;
        }
    }

    @Override
    public boolean isOpen() {
        return iterator.hasNext();
    }

    @Override
    public void close() throws IOException {}

    static int transferToDestination(ByteBuffer source, ByteBuffer destination) {
        if (source == null) {
            return 0;
        }

        val transfer = Math.min(destination.remaining(), source.remaining());
        if (transfer > 0) {
            destination.put(source.array(), source.arrayOffset() + source.position(), transfer);
            source.position(source.position() + transfer);
        }
        return transfer;
    }

    static boolean isNotEmpty(ByteBuffer buffer) {
        return buffer.hasRemaining();
    }
}
