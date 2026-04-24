/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.metadata;

import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Factory for Arrow-backed metadata result sets. This is the single entry point callers use to
 * materialise a list of {@link ColumnMetadata} + row values into a {@link StreamingResultSet},
 * replacing the historical row-based {@code DataCloudMetadataResultSet}.
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
        MetadataArrowBuilder.Result built = MetadataArrowBuilder.build(columns, rows);
        // Pass the original columns through as the metadata override so that JDBC-spec type names
        // ("TEXT", "INTEGER", "SHORT") survive the round-trip via Arrow — if we rederived from the
        // Arrow schema we would get the generic {@code HyperType}-derived names ("VARCHAR" etc.).
        return StreamingResultSet.ofInMemory(
                built.getRoot(), built, /*queryId=*/ null, ZoneId.systemDefault(), columns);
    }

    /**
     * Convenience overload for callers that still speak in terms of {@code List<Object>} where
     * each element is itself a {@code List<Object>} row. Mirrors the old
     * {@code DataCloudMetadataResultSet.of(..., List<Object> data)} signature.
     */
    public static StreamingResultSet ofRawRows(List<ColumnMetadata> columns, List<Object> rawRows) throws SQLException {
        return of(columns, coerceRows(rawRows));
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
