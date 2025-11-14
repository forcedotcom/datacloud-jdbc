/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.ColumnMetaData;

/**
 * Resolves column names to their JDBC indices using fast HashMap lookups.
 *
 * <p>This class provides O(1) column name lookup instead of O(n) linear search, which is critical
 * for performance when dealing with large numbers of columns.
 *
 * <p>Lookup strategy:
 * <ol>
 *   <li>First try exact match (case-sensitive)
 *   <li>If no exact match, try lowercase match (case-insensitive fallback)
 * </ol>
 *
 * <p>This ensures exact matches are preferred, and case-insensitive matches use the first occurrence
 * (lowest ordinal) as a tie-breaker. For duplicate column names, the first column with that name
 * (lowest ordinal) is returned.
 */
public final class ColumnNameResolver {
    private final Map<String, Integer> exactColumnLabelMap;
    private final Map<String, Integer> lowercaseColumnLabelMap;

    /**
     * Creates a new ColumnNameResolver from the given column metadata.
     *
     * @param columns the column metadata list
     */
    public ColumnNameResolver(List<ColumnMetaData> columns) {
        this.exactColumnLabelMap = new HashMap<>(columns.size());
        this.lowercaseColumnLabelMap = new HashMap<>(columns.size());

        // First pass: index exact labels to ordinals
        for (ColumnMetaData columnMetaData : columns) {
            if (columnMetaData.label != null) {
                // Use putIfAbsent to ensure first occurrence (lowest ordinal) wins for duplicates
                exactColumnLabelMap.putIfAbsent(columnMetaData.label, columnMetaData.ordinal);
            }
        }

        // Second pass: index lowercase labels to ordinals, but only if the lowercase key doesn't exist yet
        // This ensures the first column with a given lowercase name wins (lowest ordinal)
        for (ColumnMetaData columnMetaData : columns) {
            if (columnMetaData.label != null) {
                String lowerLabel = columnMetaData.label.toLowerCase();
                // Only add if this lowercase key hasn't been seen yet (preserves first occurrence)
                lowercaseColumnLabelMap.putIfAbsent(lowerLabel, columnMetaData.ordinal);
            }
        }
    }

    /**
     * Finds the JDBC column index (1-based) for the given column label.
     *
     * <p>First tries an exact case-sensitive match, then falls back to a case-insensitive match.
     *
     * @param columnLabel the column label to find
     * @return the JDBC column index (1-based)
     * @throws SQLException if the column is not found
     */
    public int findColumn(String columnLabel) throws SQLException {
        // First try exact match (case-sensitive)
        Integer index = exactColumnLabelMap.get(columnLabel);
        if (index != null) {
            // Avatica uses 0-based ordinals, but JDBC uses 1-based indices
            return index + 1;
        }

        // Fallback to lowercase match (case-insensitive)
        index = lowercaseColumnLabelMap.get(columnLabel.toLowerCase());
        if (index != null) {
            // Avatica uses 0-based ordinals, but JDBC uses 1-based indices
            return index + 1;
        }

        throw new SQLException("column '" + columnLabel + "' not found", "42703");
    }
}
