/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.protocol.QueryResultArrowStream;
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
 * results and materialised metadata results both flow through {@link DataCloudResultSet}.
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
    public static DataCloudResultSet empty(List<ColumnMetadata> columns) throws SQLException {
        return of(columns, Collections.emptyList());
    }

    /** Empty result set with no columns — used as a placeholder by unsupported metadata methods. */
    public static DataCloudResultSet emptyNoColumns() throws SQLException {
        return of(Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Build a result set whose schema is {@code columns} and whose rows are {@code rows}. Each
     * inner list in {@code rows} supplies values in column order, and must have exactly
     * {@code columns.size()} elements — a short row would silently leave the trailing columns
     * unset (interpreted as Arrow null), which is almost always a caller bug. Today every caller
     * goes through {@link MetadataSchemas} so the sizes match by construction; the precondition
     * here makes a future caller bug surface at the boundary instead of in vector population.
     */
    public static DataCloudResultSet of(List<ColumnMetadata> columns, List<List<Object>> rows) throws SQLException {
        validateRowArity(columns, rows);
        byte[] ipcBytes = writeArrowStream(columns, rows);
        // Reuse the query-path allocator budget so a future caller materialising a multi-MB
        // metadata response trips the cap cleanly instead of letting the JVM OOM.
        RootAllocator allocator = new RootAllocator(QueryResultArrowStream.ROOT_ALLOCATOR_BUDGET_BYTES);
        ArrowStreamReader reader;
        try {
            reader = new ArrowStreamReader(new ByteArrayInputStream(ipcBytes), allocator);
        } catch (Throwable t) {
            // Constructor-time leak guard: if ArrowStreamReader fails before DataCloudResultSet.of
            // can take ownership, close the allocator on the way out.
            try {
                allocator.close();
            } catch (Throwable s) {
                t.addSuppressed(s);
            }
            throw t;
        }
        // Allocator and reader are now handed to DataCloudResultSet, which owns their lifecycle
        // and closes both on close() — including the construction-failure path inside of(...).
        return DataCloudResultSet.of(
                new QueryResultArrowStream.Result(reader, allocator), /*queryId=*/ null, ZoneId.systemDefault());
    }

    /**
     * Convenience overload for callers that still speak in terms of {@code List<Object>} where
     * each element is itself a {@code List<Object>} row. Mirrors the old
     * {@code DataCloudMetadataResultSet.of(..., List<Object> data)} signature.
     */
    public static DataCloudResultSet ofRawRows(List<ColumnMetadata> columns, List<Object> rawRows) throws SQLException {
        return of(columns, coerceRows(rawRows));
    }

    /**
     * Build the Arrow schema, populate a VSR via the shared {@link VectorPopulator}, and write it
     * out as a single-batch Arrow IPC stream.
     */
    private static byte[] writeArrowStream(List<ColumnMetadata> columns, List<List<Object>> rows) throws SQLException {
        Schema schema = new Schema(columns.stream()
                .map(c -> HyperTypeToArrow.toField(c.getName(), c.getType()))
                .collect(Collectors.toList()));
        try (RootAllocator writeAllocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, writeAllocator)) {
            root.allocateNew();
            VectorPopulator.populateVectors(root, columns, rows, /*calendar=*/ null);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        } catch (IOException ex) {
            throw new SQLException("Failed to build metadata result set", "XX000", ex);
        }
    }

    /**
     * Verify that every supplied row has exactly {@code columns.size()} elements. A {@code null}
     * row is allowed and is interpreted as a row of all-nulls (matching the old
     * {@code coerceRows} convention of converting null rows to empty lists, which is the only
     * shape with no positional values to populate). Anything else is a caller bug.
     */
    private static void validateRowArity(List<ColumnMetadata> columns, List<List<Object>> rows) throws SQLException {
        int expected = columns.size();
        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            if (row == null) {
                continue;
            }
            // The legacy coerceRows path turns a null-row into Collections.emptyList(); accept
            // empty as the "all nulls" shape here too.
            if (row.isEmpty() && expected > 0) {
                continue;
            }
            if (row.size() != expected) {
                throw new SQLException("Metadata row " + i + " has " + row.size() + " elements but schema has "
                        + expected + " columns");
            }
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
