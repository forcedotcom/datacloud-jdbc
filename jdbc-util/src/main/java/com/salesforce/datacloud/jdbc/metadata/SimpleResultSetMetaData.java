package com.salesforce.datacloud.jdbc.metadata;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SimpleResultSetMetaData implements ResultSetMetaData {
    /// The columns
    private final ColumnMetadata[] columns;

    public SimpleResultSetMetaData(ColumnMetadata[] columns) {
        this.columns = columns;
    }

    /// Find a column by label
    /// Proprietary extension to the JDBC API
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getName().equals(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column label " + columnLabel + " not found");
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length;
    }

    /// Get a column by index
    public ColumnMetadata getColumn(int column) throws SQLException {
        if (column <= 0 || column >= columns.length) {
            throw new SQLException("Column index " + column + " out of bounds (" + columns.length + " columns available)");
        }
        // Column indices are 1-based in JDBC
        return columns[column - 1];
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).getName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).getType().getJdbcType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getType().toString();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumn(column).getType().getJavaTypeName();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumn(column).getType().isSigned();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).getType().getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).getType().getScale();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumn(column).getType().isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumn(column).getType().getDisplaySize();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
        return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    /////////////////////////////
    /// Lineage-related methods
    /// We don't support lineage for our result sets (yet)
    /// The following methods are inspired by Postgres's ResultSetMetaData.

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnLabel(column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    /////////////////////////////
    // Write-related methods
    // We don't support writing to our result sets

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        // Our result sets are always read only
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        // Our result sets are always read only
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        // Our result sets are always read only
        return false;
    }

    /////////////////////////////
    // Other functionality not relevant to our result sets

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }
}