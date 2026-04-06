/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.core.metadata.ColumnMetadata;
import com.salesforce.datacloud.jdbc.core.metadata.SimpleResultSetMetaData;
import com.salesforce.datacloud.jdbc.core.resultset.ColumnAccessor;
import com.salesforce.datacloud.jdbc.core.resultset.SimpleResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Custom ResultSet implementation for metadata queries
 */
public class SimpleMetadataResultSet extends SimpleResultSet<SimpleMetadataResultSet> {

    private final List<Object> data;
    private int currentRow = -1;
    private boolean closed = false;

    private SimpleMetadataResultSet(
            SimpleResultSetMetaData metadata, ColumnAccessor<SimpleMetadataResultSet>[] accessors, List<Object> data) {
        super(metadata, accessors, false);
        this.data = data;
    }

    public static SimpleMetadataResultSet empty() throws SQLException {
        return of(new SimpleResultSetMetaData(Collections.emptyList()), Collections.emptyList());
    }

    public static SimpleMetadataResultSet of(SimpleResultSetMetaData metadata, List<Object> data) throws SQLException {
        @SuppressWarnings("unchecked")
        ColumnAccessor<SimpleMetadataResultSet>[] accessors = new ColumnAccessor[metadata.getColumnCount()];
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            final int columnIndex = i;
            accessors[i] = createAccessor(metadata.getColumn(i + 1), columnIndex);
        }

        return new SimpleMetadataResultSet(metadata, accessors, data);
    }

    /**
     * Creates a ColumnAccessor for a specific column.
     */
    private static ColumnAccessor<SimpleMetadataResultSet> createAccessor(ColumnMetadata column, int columnIndex) {
        return new ColumnAccessor<SimpleMetadataResultSet>() {
            @Override
            public String getString(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                return value.toString();
            }

            @Override
            public Boolean getBoolean(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof Boolean) {
                    return (Boolean) value;
                }
                return false;
            }

            @Override
            public OptionalLong getAnyInteger(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return OptionalLong.empty();
                }
                if (value instanceof Number) {
                    return OptionalLong.of(((Number) value).longValue());
                }
                throw new SQLException(
                        "Cannot convert to integer: " + value.getClass().getName());
            }

            /**
             * Helper method to get the value for the current row and column.
             */
            private Object getValue(SimpleMetadataResultSet resultSet, int columnIndex) throws SQLException {
                if (resultSet.closed) {
                    throw new SQLException("ResultSet is closed");
                }
                if (resultSet.currentRow < 0) {
                    throw new SQLException("No current row. Call next() first.");
                }
                if (resultSet.currentRow >= resultSet.data.size()) {
                    throw new SQLException("Row index out of bounds");
                }

                Object row = resultSet.data.get(resultSet.currentRow);
                if (!(row instanceof List)) {
                    throw new SQLException("Data row is not a List");
                }

                @SuppressWarnings("unchecked")
                List<Object> rowList = (List<Object>) row;

                if (columnIndex >= rowList.size()) {
                    throw new SQLException("Column index " + columnIndex + " out of bounds for row");
                }

                return rowList.get(columnIndex);
            }
        };
    }

    @Override
    public boolean next() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
        currentRow++;
        return currentRow < data.size();
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public int getRow() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
        if (currentRow >= 0 && currentRow < data.size()) {
            return currentRow + 1;
        }
        return 0;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
