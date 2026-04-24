/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperTypeToArrow;
import com.salesforce.datacloud.jdbc.protocol.data.VectorPopulator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Factory for Arrow-backed metadata result sets. Materialises a row-oriented list of metadata
 * values into the Arrow IPC format used by every other driver result set, so streaming query
 * results and materialised metadata results both flow through {@link StreamingResultSet}.
 *
 * <p>Each call builds a fresh single-batch Arrow stream: a writer-side {@link VectorSchemaRoot}
 * is populated via {@link VectorPopulator} (the same code path the JDBC parameter encoder uses),
 * serialised to bytes, and wrapped in an {@link ArrowStreamReader} that the result set owns.
 */
public final class MetadataResultSets {

    private MetadataResultSets() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /** Empty result set with the given column schema. */
    public static StreamingResultSet empty(List<ColumnMetadata> columns) throws SQLException {
        return of(columns, Collections.emptyList());
    }

    /** Empty result set with no columns — used as a placeholder by unsupported metadata methods. */
    public static StreamingResultSet emptyNoColumns() throws SQLException {
        return of(Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Build a result set whose schema is {@code columns} and whose rows are {@code rows}. Each
     * inner list in {@code rows} supplies values in column order.
     */
    public static StreamingResultSet of(List<ColumnMetadata> columns, List<List<Object>> rows) throws SQLException {
        byte[] ipcBytes = writeArrowStream(columns, rows);
        // Allocator is handed to StreamingResultSet along with the reader; the result set owns
        // its lifecycle and closes it when close() is called.
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try {
            ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipcBytes), allocator);
            return StreamingResultSet.of(reader, allocator, /*queryId=*/ null, ZoneId.systemDefault());
        } catch (SQLException | RuntimeException ex) {
            allocator.close();
            throw ex;
        }
    }

    /**
     * Convenience overload for callers that still speak in terms of {@code List<Object>} where
     * each element is itself a {@code List<Object>} row. Mirrors the old
     * {@code DataCloudMetadataResultSet.of(..., List<Object> data)} signature.
     */
    public static StreamingResultSet ofRawRows(List<ColumnMetadata> columns, List<Object> rawRows) throws SQLException {
        return of(columns, coerceRows(rawRows));
    }

    /**
     * Build the Arrow schema, populate a VSR via the shared {@link VectorPopulator}, and write it
     * out as a single-batch Arrow IPC stream.
     */
    private static byte[] writeArrowStream(List<ColumnMetadata> columns, List<List<Object>> rows) throws SQLException {
        Schema schema = new Schema(columns.stream()
                .map(c -> HyperTypeToArrow.toField(c.getName(), c.getType(), c.getTypeName()))
                .collect(Collectors.toList()));
        try (RootAllocator writeAllocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, writeAllocator)) {
            root.allocateNew();
            VectorPopulator.populateVectors(root, columns, rows, /*calendar=*/ null);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                // Skip writeBatch() for empty results — writing a zero-row batch confuses the
                // cursor (see ArrowStreamReaderCursor.next), which interprets a successfully
                // loaded batch as "at least one row available".
                if (root.getRowCount() > 0) {
                    writer.writeBatch();
                }
                writer.end();
            }
            return out.toByteArray();
        } catch (IOException ex) {
            throw new SQLException("Failed to build metadata result set", "XX000", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> coerceRows(List<Object> rawRows) throws SQLException {
        if (rawRows == null || rawRows.isEmpty()) {
            return Collections.emptyList();
        }
        List<List<Object>> result = new ArrayList<>(rawRows.size());
        for (Object row : rawRows) {
            if (row == null) {
                result.add(Collections.emptyList());
            } else if (row instanceof List) {
                result.add((List<Object>) row);
            } else {
                throw new SQLException(
                        "Metadata row is not a List: " + row.getClass().getName());
            }
        }
        return result;
    }
}
