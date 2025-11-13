/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.metadata.ColumnMetadata;
import com.salesforce.datacloud.jdbc.metadata.SimpleResultSetMetaData;
import com.salesforce.datacloud.jdbc.metadata.SqlType;
import com.salesforce.datacloud.jdbc.resultset.ColumnAccessor;
import com.salesforce.datacloud.jdbc.resultset.SimpleResultSet;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * Custom ResultSet implementation for metadata queries
 */
public class SimpleMetadataResultSet extends SimpleResultSet<SimpleMetadataResultSet> {

    private final List<Object> data;
    private int currentRow = -1;
    private boolean closed = false;

    private SimpleMetadataResultSet(
            SimpleResultSetMetaData metadata,
            ColumnAccessor<SimpleMetadataResultSet>[] accessors,
            List<Object> data) {
        super(metadata, accessors, false);
        this.data = data;
    }

    public static SimpleMetadataResultSet of(QueryDBMetadata queryDbMetadata, List<Object> data)
            throws SQLException {
        ColumnMetadata[] columns = convertToColumnMetadata(queryDbMetadata);
        SimpleResultSetMetaData metadata = new SimpleResultSetMetaData(columns);
        @SuppressWarnings("unchecked")
        ColumnAccessor<SimpleMetadataResultSet>[] accessors = new ColumnAccessor[columns.length];
        for (int i = 0; i < columns.length; i++) {
            final int columnIndex = i;
            accessors[i] = createAccessor(columns[i], columnIndex);
        }
        
        return new SimpleMetadataResultSet(metadata, accessors, data);
    }

    private static ColumnMetadata[] convertToColumnMetadata(QueryDBMetadata queryDbMetadata) {
        List<String> columnNames = queryDbMetadata.getColumnNames();
        List<Integer> columnTypeIds = queryDbMetadata.getColumnTypeIds();
        List<String> columnTypes = queryDbMetadata.getColumnTypes();
        
        ColumnMetadata[] columns = new ColumnMetadata[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            String columnType = columnTypes.get(i);
            int jdbcType = columnTypeIds.get(i);
            SqlType sqlType = jdbcTypeToSqlType(jdbcType);
            columns[i] = new ColumnMetadata(name, sqlType, columnType);
        }
        return columns;
    }

    private static SqlType jdbcTypeToSqlType(int jdbcType) {
        switch (jdbcType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return SqlType.createBool();
            case Types.SMALLINT:
            case Types.TINYINT:
                return SqlType.createSmallInt();
            case Types.INTEGER:
                return SqlType.createInteger();
            case Types.BIGINT:
                return SqlType.createBigInt();
            case Types.NUMERIC:
            case Types.DECIMAL:
                // Default precision/scale for metadata queries
                return SqlType.createNumeric(38, 18);
            case Types.REAL:
            case Types.FLOAT:
                return SqlType.createFloat();
            case Types.DOUBLE:
                return SqlType.createDouble();
            case Types.CHAR:
                return SqlType.createChar(255); // Default length
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return SqlType.createVarchar(0); // Unlimited length
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return SqlType.createBytea();
            case Types.DATE:
                return SqlType.createDate();
            case Types.TIME:
                return SqlType.createTime();
            case Types.TIMESTAMP:
                return SqlType.createTimestamp();
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return SqlType.createTimestampTZ();
            case Types.ARRAY:
                // For arrays, use VARCHAR as element type (common in metadata)
                return SqlType.createArray(SqlType.createVarchar(0));
            default:
                // Default to VARCHAR for unknown types
                return SqlType.createVarchar(0);
        }
    }

    /**
     * Creates a ColumnAccessor for a specific column.
     * TODO: Pull out into own class
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
                if (value instanceof String) {
                    String str = ((String) value).trim().toLowerCase();
                    return "true".equals(str) || "yes".equals(str) || "1".equals(str);
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
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
                if (value instanceof String) {
                    try {
                        return OptionalLong.of(Long.parseLong((String) value));
                    } catch (NumberFormatException e) {
                        throw new SQLException("Cannot convert to integer: " + value, e);
                    }
                }
                throw new SQLException("Cannot convert to integer: " + value.getClass().getName());
            }

            @Override
            public BigDecimal getBigDecimal(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof BigDecimal) {
                    return (BigDecimal) value;
                }
                if (value instanceof Number) {
                    return BigDecimal.valueOf(((Number) value).doubleValue());
                }
                if (value instanceof String) {
                    try {
                        return new BigDecimal((String) value);
                    } catch (NumberFormatException e) {
                        throw new SQLException("Cannot convert to BigDecimal: " + value, e);
                    }
                }
                throw new SQLException("Cannot convert to BigDecimal: " + value.getClass().getName());
            }

            @Override
            public OptionalDouble getAnyFloatingPoint(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return OptionalDouble.empty();
                }
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                if (value instanceof String) {
                    try {
                        return OptionalDouble.of(Double.parseDouble((String) value));
                    } catch (NumberFormatException e) {
                        throw new SQLException("Cannot convert to floating point: " + value, e);
                    }
                }
                throw new SQLException("Cannot convert to floating point: " + value.getClass().getName());
            }

            @Override
            public byte[] getBytes(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof byte[]) {
                    return (byte[]) value;
                }
                if (value instanceof String) {
                    return ((String) value).getBytes();
                }
                throw new SQLException("Cannot convert to byte array: " + value.getClass().getName());
            }

            @Override
            public Date getDate(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof Date) {
                    return (Date) value;
                }
                if (value instanceof java.util.Date) {
                    return new Date(((java.util.Date) value).getTime());
                }
                if (value instanceof String) {
                    try {
                        return Date.valueOf((String) value);
                    } catch (IllegalArgumentException e) {
                        throw new SQLException("Cannot convert to Date: " + value, e);
                    }
                }
                throw new SQLException("Cannot convert to Date: " + value.getClass().getName());
            }

            @Override
            public Time getTime(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof Time) {
                    return (Time) value;
                }
                if (value instanceof java.util.Date) {
                    return new Time(((java.util.Date) value).getTime());
                }
                if (value instanceof String) {
                    try {
                        return Time.valueOf((String) value);
                    } catch (IllegalArgumentException e) {
                        throw new SQLException("Cannot convert to Time: " + value, e);
                    }
                }
                throw new SQLException("Cannot convert to Time: " + value.getClass().getName());
            }

            @Override
            public Timestamp getTimestamp(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof Timestamp) {
                    return (Timestamp) value;
                }
                if (value instanceof java.util.Date) {
                    return new Timestamp(((java.util.Date) value).getTime());
                }
                if (value instanceof String) {
                    try {
                        return Timestamp.valueOf((String) value);
                    } catch (IllegalArgumentException e) {
                        throw new SQLException("Cannot convert to Timestamp: " + value, e);
                    }
                }
                throw new SQLException("Cannot convert to Timestamp: " + value.getClass().getName());
            }

            @Override
            public Array getArray(SimpleMetadataResultSet resultSet) throws SQLException {
                Object value = getValue(resultSet, columnIndex);
                if (value == null) {
                    return null;
                }
                if (value instanceof Array) {
                    return (Array) value;
                }
                if (value instanceof List) {
                    // Convert List to Array if needed
                    throw new SQLException("List to Array conversion not yet implemented");
                }
                throw new SQLException("Cannot convert to Array: " + value.getClass().getName());
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

