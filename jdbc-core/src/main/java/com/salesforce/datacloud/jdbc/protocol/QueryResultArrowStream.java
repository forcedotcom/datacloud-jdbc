/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.salesforce.datacloud.jdbc.core.ByteStringReadableByteChannel;
import lombok.val;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import salesforce.cdp.hyperdb.v1.OutputFormat;
import salesforce.cdp.hyperdb.v1.QueryResult;

public class QueryResultArrowStream {
    /**
     * This is the canonical output format for Arrow query results
     */
    public static final OutputFormat OUTPUT_FORMAT = OutputFormat.ARROW_IPC;

    private static final int ROOT_ALLOCATOR_MB_FROM_V2 = 100 * 1024 * 1024;

    public static ArrowStreamReader toArrowStreamReader(CloseableIterator<QueryResult> iterator) {
        val byteStringIterator = FluentIterable.from(() -> iterator)
                .transform(
                        input -> input.hasBinaryPart() ? input.getBinaryPart().getData() : null)
                .filter(Predicates.notNull())
                .iterator();
        // Wrap the derived ByteString iterator with the original iterator's close behavior
        // so that closing the ArrowStreamReader cancels the underlying gRPC stream.
        CloseableIterator<com.google.protobuf.ByteString> closeable =
                new CloseableIterator<com.google.protobuf.ByteString>() {
                    @Override
                    public boolean hasNext() {
                        return byteStringIterator.hasNext();
                    }

                    @Override
                    public com.google.protobuf.ByteString next() {
                        return byteStringIterator.next();
                    }

                    @Override
                    public void close() throws Exception {
                        iterator.close();
                    }
                };
        val channel = new ByteStringReadableByteChannel(closeable);
        return new ArrowStreamReader(channel, new RootAllocator(ROOT_ALLOCATOR_MB_FROM_V2));
    }
}
