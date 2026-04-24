/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.ThrowingFunction.rethrowFunction;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * Row cursor over a {@link VectorSchemaRoot} that drives the {@link StreamingResultSet}.
 *
 * <p>The cursor is source-agnostic: a {@link BatchLoader} loads the next batch into the vector
 * schema root, whether that comes from an Arrow IPC stream or a pre-populated in-memory batch.
 * This is the single codepath that the driver exposes to JDBC callers — streaming query results
 * and materialised metadata results both funnel through here.
 *
 * <p>The cursor owns an optional {@link AutoCloseable} holding the resources that back the
 * vector schema root (allocator, underlying reader, etc.). Closing the cursor closes that holder,
 * guaranteeing root-allocator hygiene without requiring each call site to manage the allocator
 * separately.
 */
@Slf4j
class ArrowStreamReaderCursor implements AutoCloseable {

    private static final int INIT_ROW_NUMBER = -1;

    private final VectorSchemaRoot root;
    private final BatchLoader batchLoader;
    private final AutoCloseable ownedResources;
    private final ZoneId sessionZone;

    @lombok.Getter
    private int rowsSeen = 0;

    private final AtomicInteger currentIndex = new AtomicInteger(INIT_ROW_NUMBER);

    /**
     * Loads the next batch of rows into the vector schema root.
     *
     * <p>Implementations should return {@code true} if the vector schema root now holds rows from
     * a newly-loaded batch, and {@code false} if the source has no more data.
     */
    @FunctionalInterface
    interface BatchLoader {
        boolean loadNextBatch() throws Exception;
    }

    /**
     * Create a cursor that pulls batches from an {@link ArrowStreamReader}. The reader (and the
     * allocator it was constructed with) are owned by the cursor — closing the cursor closes the
     * supplied {@code ownedResources}.
     */
    @SneakyThrows
    static ArrowStreamReaderCursor streaming(
            ArrowStreamReader reader, AutoCloseable ownedResources, ZoneId sessionZone) {
        val root = reader.getVectorSchemaRoot();
        BatchLoader loader = reader::loadNextBatch;
        return new ArrowStreamReaderCursor(root, loader, ownedResources, sessionZone);
    }

    /**
     * Create a cursor over a single pre-populated {@link VectorSchemaRoot}. The root (and any
     * backing allocator wrapped in {@code ownedResources}) are owned by the cursor — closing the
     * cursor closes the supplied {@code ownedResources}.
     */
    static ArrowStreamReaderCursor inMemory(VectorSchemaRoot root, AutoCloseable ownedResources, ZoneId sessionZone) {
        // The VSR is already populated, so there is nothing more to load — the cursor walks the
        // row count until exhausted and then reports end-of-stream.
        return new ArrowStreamReaderCursor(root, () -> false, ownedResources, sessionZone);
    }

    private ArrowStreamReaderCursor(
            VectorSchemaRoot root, BatchLoader batchLoader, AutoCloseable ownedResources, ZoneId sessionZone) {
        this.root = root;
        this.batchLoader = batchLoader;
        this.ownedResources = ownedResources;
        this.sessionZone = sessionZone;
    }

    List<QueryJDBCAccessor> createAccessors() {
        return root.getFieldVectors().stream()
                .map(rethrowFunction(this::createAccessor))
                .collect(Collectors.toList());
    }

    private QueryJDBCAccessor createAccessor(FieldVector vector) throws SQLException {
        return QueryJDBCAccessorFactory.createAccessor(vector, currentIndex::get, sessionZone);
    }

    private boolean loadNextBatch() throws Exception {
        if (batchLoader.loadNextBatch()) {
            currentIndex.set(0);
            return true;
        }
        return false;
    }

    @SneakyThrows
    public boolean next() {
        val current = currentIndex.incrementAndGet();
        val total = root.getRowCount();

        try {
            val next = current < total || loadNextBatch();
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

    @SneakyThrows
    @Override
    public void close() {
        if (ownedResources != null) {
            ownedResources.close();
        }
    }
}
