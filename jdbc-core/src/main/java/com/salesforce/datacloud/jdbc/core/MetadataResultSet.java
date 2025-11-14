/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.val;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

public class MetadataResultSet extends AvaticaResultSet {
    // Fast lookup maps for column names
    // exactMap: exact label -> ordinal (for case-sensitive exact matches)
    // lowercaseMap: lowercase label -> ordinal (for case-insensitive fallback, first occurrence wins)
    private volatile Map<String, Integer> exactColumnLabelMap;
    private volatile Map<String, Integer> lowercaseColumnLabelMap;

    public MetadataResultSet(
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame)
            throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        // Build fast lookup maps for column names
        buildColumnLabelMaps(signature.columns);
    }

    private void buildColumnLabelMaps(List<ColumnMetaData> columns) {
        Map<String, Integer> exactMap = new HashMap<>(columns.size());
        Map<String, Integer> lowercaseMap = new HashMap<>(columns.size());

        // First pass: index exact labels to ordinals
        for (ColumnMetaData columnMetaData : columns) {
            if (columnMetaData.label != null) {
                exactMap.put(columnMetaData.label, columnMetaData.ordinal);
            }
        }

        // Second pass: index lowercase labels to ordinals, but only if the lowercase key doesn't exist yet
        // This ensures the first column with a given lowercase name wins (lowest ordinal)
        for (ColumnMetaData columnMetaData : columns) {
            if (columnMetaData.label != null) {
                String lowerLabel = columnMetaData.label.toLowerCase();
                // Only add if this lowercase key hasn't been seen yet (preserves first occurrence)
                lowercaseMap.putIfAbsent(lowerLabel, columnMetaData.ordinal);
            }
        }

        this.exactColumnLabelMap = exactMap;
        this.lowercaseColumnLabelMap = lowercaseMap;
    }

    public static AvaticaResultSet of() throws SQLException {
        val signature = new Meta.Signature(
                ImmutableList.of(), null, ImmutableList.of(), ImmutableMap.of(), null, Meta.StatementType.SELECT);
        return of(
                null,
                new QueryState(),
                signature,
                new AvaticaResultSetMetaData(null, null, signature),
                TimeZone.getDefault(),
                null,
                ImmutableList.of());
    }

    public static AvaticaResultSet of(
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame,
            List<Object> data)
            throws SQLException {
        AvaticaResultSet result;
        result = new MetadataResultSet(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        result.execute2(new MetadataCursor(data), signature.columns);
        return result;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    /**
     * Override findColumn to use HashMap for O(1) lookup instead of O(n) linear search.
     * This is critical for performance when dealing with large numbers of columns.
     *
     * Lookup strategy:
     * 1. First try exact match (case-sensitive)
     * 2. If no exact match, try lowercase match (case-insensitive fallback)
     *
     * This ensures exact matches are preferred, and case-insensitive matches use the first
     * occurrence (lowest ordinal) as a tie-breaker.
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Map<String, Integer> exactMap = exactColumnLabelMap;
        Map<String, Integer> lowercaseMap = lowercaseColumnLabelMap;

        if (exactMap == null || lowercaseMap == null) {
            // Fallback to parent implementation if maps not initialized
            return super.findColumn(columnLabel);
        }

        // First try exact match (case-sensitive)
        Integer index = exactMap.get(columnLabel);
        if (index != null) {
            return index + 1;
        }

        // Fallback to lowercase match (case-insensitive)
        index = lowercaseMap.get(columnLabel.toLowerCase());
        if (index != null) {
            return index + 1;
        }

        throw AvaticaConnection.HELPER.createException("column '" + columnLabel + "' not found");
    }

    /**
     * Override getter methods that take String columnLabel to ensure they use our optimized findColumn() method.
     * This is necessary because Avatica's implementation might use a private method or cache.
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getString(columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getInt(columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getLong(columnIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBoolean(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getByte(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getShort(columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getFloat(columnIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDouble(columnIndex);
    }
}
