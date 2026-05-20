/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.salesforce.datacloud.jdbc.core.ByteStringReadableByteChannel;
import lombok.Value;
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

    /**
     * Per-result-set allocator budget. Hitting this threshold trips a clean
     * {@link org.apache.arrow.memory.OutOfMemoryException} from the allocator instead of letting
     * the JVM OOM. Reused for the metadata-side allocator in
     * {@link com.salesforce.datacloud.jdbc.core.metadata.MetadataResultSets}.
     */
    public static final int ROOT_ALLOCATOR_BUDGET_BYTES = 100 * 1024 * 1024;

    /**
     * Pair of the {@link ArrowStreamReader} that decodes gRPC chunks and the {@link RootAllocator}
     * that backs it. Callers hand ownership to {@link
     * com.salesforce.datacloud.jdbc.core.DataCloudResultSet#of} which closes both; the pair is
     * never closed directly.
     */
    @Value
    public static class Result {
        ArrowStreamReader reader;
        RootAllocator allocator;
    }

    public static Result toArrowStreamReader(CloseableIterator<QueryResult> iterator) {
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
        RootAllocator allocator = new RootAllocator(ROOT_ALLOCATOR_BUDGET_BYTES);
        try {
            return new Result(new ArrowStreamReader(channel, allocator), allocator);
        } catch (Throwable t) {
            // ArrowStreamReader's constructor is benign today, but a future Arrow upgrade could
            // add constructor-side validation. Close the allocator on the way out so the budget
            // is reclaimed.
            try {
                allocator.close();
            } catch (Throwable s) {
                t.addSuppressed(s);
            }
            throw t;
        }
    }
}
