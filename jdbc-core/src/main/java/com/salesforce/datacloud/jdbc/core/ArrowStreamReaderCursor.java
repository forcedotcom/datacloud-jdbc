/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.ThrowingFunction.rethrowFunction;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * Row cursor over an {@link ArrowStreamReader} that drives the {@link DataCloudResultSet}.
 *
 * <p>The cursor owns the supplied {@link BufferAllocator} alongside the reader: closing the
 * cursor closes the reader (which releases ArrowBuf accounting) and then the allocator (which
 * returns its budget). This is the single place that guarantees root-allocator hygiene for the
 * driver; callers of {@link DataCloudResultSet#of} hand ownership over and do not close the
 * allocator themselves.
 */
@Slf4j
class ArrowStreamReaderCursor implements AutoCloseable {

    private static final int INIT_ROW_NUMBER = -1;

    private final ArrowStreamReader reader;
    private final BufferAllocator allocator;
    private final ZoneId sessionZone;

    @lombok.Getter
    private int rowsSeen = 0;

    private final AtomicInteger currentIndex = new AtomicInteger(INIT_ROW_NUMBER);

    /**
     * Whether the first non-empty batch has been requested from the stream, either by
     * {@link #isBeforeFirst()} peeking at it or by the first {@link #next()}.
     */
    private boolean firstBatchLoaded = false;

    /**
     * Whether the stream has no further rows: the first batch was found empty, or {@link #next()}
     * ran past the last row.
     */
    private boolean endOfStream = false;

    ArrowStreamReaderCursor(ArrowStreamReader reader, BufferAllocator allocator, ZoneId sessionZone)
            throws SQLException {
        this.reader = reader;
        this.allocator = allocator;
        this.sessionZone = sessionZone;
        // next() and isBeforeFirst() assume the first batch has not been read yet; a preloaded
        // batch would be silently skipped.
        if (getSchemaRoot().getRowCount() != 0) {
            throw new SQLException("Arrow reader must not contain a loaded batch");
        }
    }

    @SneakyThrows
    private VectorSchemaRoot getSchemaRoot() {
        return reader.getVectorSchemaRoot();
    }

    List<QueryJDBCAccessor> createAccessors() {
        return getSchemaRoot().getFieldVectors().stream()
                .map(rethrowFunction(this::createAccessor))
                .collect(Collectors.toList());
    }

    private QueryJDBCAccessor createAccessor(FieldVector vector) throws SQLException {
        return QueryJDBCAccessorFactory.createAccessor(vector, currentIndex::get, sessionZone);
    }

    /**
     * Load the next batch that has at least one row, skipping any zero-row batches in between.
     * This is the only place that reads from the stream, so it also maintains
     * {@link #firstBatchLoaded} and {@link #endOfStream}.
     */
    private boolean loadNextNonEmptyBatch() throws IOException {
        firstBatchLoaded = true;
        while (reader.loadNextBatch()) {
            if (getSchemaRoot().getRowCount() > 0) {
                currentIndex.set(0);
                return true;
            }
        }
        endOfStream = true;
        return false;
    }

    @SneakyThrows
    public boolean next() {
        if (endOfStream) {
            return false;
        }
        val current = currentIndex.incrementAndGet();
        val total = getSchemaRoot().getRowCount();

        try {
            val next = current < total || loadNextNonEmptyBatch();
            if (next) {
                rowsSeen++;
            }
            return next;
        } catch (Exception e) {
            // This can happen due to SneakyThrows.
            if (e instanceof SQLException) {
                throw e;
            } else {
                throw new SQLException("Failed to load next batch: " + e.getMessage(), e);
            }
        }
    }

    /**
     * True if no row has been returned yet and the stream has at least one row to return.
     *
     * <p>Answering this for an empty result requires reading the first non-empty batch, so the
     * first call peeks at it, blocking on the stream just as the first {@link #next()} would. The
     * batch stays loaded and the cursor stays before row 0. Peeking is only safe while no row has
     * been returned, since loading a batch replaces the vectors backing the current row.
     */
    @SneakyThrows
    boolean isBeforeFirst() {
        if (rowsSeen > 0 || endOfStream) {
            return false;
        }
        if (!firstBatchLoaded && next()) {
            // next() consumed row 0 to load the batch; step back so the caller's next() returns it.
            currentIndex.set(INIT_ROW_NUMBER);
            rowsSeen--;
        }
        return !endOfStream;
    }

    /**
     * True once {@link #next()} has moved past the last row. False for an empty result, which has
     * no last row to be after.
     */
    boolean isAfterLast() {
        return endOfStream && rowsSeen > 0;
    }

    /** True while positioned on the first row. */
    boolean isFirst() {
        return rowsSeen == 1 && !endOfStream;
    }

    @SneakyThrows
    @Override
    public void close() {
        // try-with-resources closes in reverse declaration order: reader first (releases the
        // buffers accounted against the allocator so its closing budget check passes), then
        // allocator. If both throw, Java attaches the second as suppressed onto the first
        // instead of dropping the reader exception via the standard try/finally semantics.
        try (BufferAllocator a = allocator;
                ArrowStreamReader r = reader) {
            // resource cleanup happens at exit
        }
    }
}
