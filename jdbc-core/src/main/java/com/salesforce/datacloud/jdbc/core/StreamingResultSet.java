/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.metadata.ColumnMetadata;
import com.salesforce.datacloud.jdbc.core.metadata.DataCloudResultSetMetaData;
import com.salesforce.datacloud.jdbc.core.resultset.ForwardOnlyResultSet;
import com.salesforce.datacloud.jdbc.core.resultset.ReadOnlyResultSet;
import com.salesforce.datacloud.jdbc.core.resultset.ResultSetWithPositionalGetters;
import com.salesforce.datacloud.jdbc.util.ArrowToColumnTypeMapper;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import com.salesforce.datacloud.query.v3.QueryStatus;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

@Slf4j
public class StreamingResultSet
        implements DataCloudResultSet, ReadOnlyResultSet, ForwardOnlyResultSet, ResultSetWithPositionalGetters {

    @Getter
    private final String queryId;

    private final ArrowStreamReaderCursor cursor;
    private final QueryJDBCAccessor[] accessors;
    private final DataCloudResultSetMetaData metadata;
    private final ColumnNameResolver columnNameResolver;
    ThrowingJdbcSupplier<QueryStatus> getQueryStatus;
    private boolean wasNull;
    private boolean closed;

    private StreamingResultSet(
            ArrowStreamReaderCursor cursor,
            String queryId,
            DataCloudResultSetMetaData metadata,
            QueryJDBCAccessor[] accessors,
            ColumnNameResolver columnNameResolver) {
        this.cursor = cursor;
        this.queryId = queryId;
        this.metadata = metadata;
        this.accessors = accessors;
        this.columnNameResolver = columnNameResolver;
        this.closed = false;
    }

    public static StreamingResultSet of(ArrowStreamReader resultStream, String queryId) throws SQLException {
        return of(resultStream, queryId, ZoneId.systemDefault());
    }

    /**
     * Creates a StreamingResultSet with a specified session timezone.
     *
     * @param resultStream The Arrow stream containing query results
     * @param queryId The query identifier
     * @param sessionZone The session timezone to use for timestamp conversions
     * @return A new StreamingResultSet
     * @throws SQLException If an error occurs during ResultSet creation
     */
    public static StreamingResultSet of(ArrowStreamReader resultStream, String queryId, ZoneId sessionZone)
            throws SQLException {
        try {
            val schemaRoot = resultStream.getVectorSchemaRoot();
            val fields = schemaRoot.getSchema().getFields();

            val columns = fields.stream()
                    .map(field -> {
                        val type = ArrowToColumnTypeMapper.toColumnType(field);
                        return new ColumnMetadata(
                                field.getName(), type, type.getType().getName());
                    })
                    .collect(Collectors.toList());
            val metadata = new DataCloudResultSetMetaData(columns);

            val cursor = new ArrowStreamReaderCursor(resultStream, sessionZone);
            val accessorList = cursor.createAccessors();
            val accessors = accessorList.toArray(new QueryJDBCAccessor[0]);

            val columnNameResolver = new ColumnNameResolver(columns);

            return new StreamingResultSet(cursor, queryId, metadata, accessors, columnNameResolver);
        } catch (IOException ex) {
            throw new SQLException("Unexpected error during ResultSet creation", "XX000", ex);
        }
    }

    // --- Core ResultSet navigation ---

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        return cursor.next();
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            cursor.close();
            closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return cursor.getRowsSeen();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metadata;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        return columnNameResolver.findColumn(columnLabel);
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    // --- Accessor dispatch: delegate to QueryJDBCAccessor ---

    private QueryJDBCAccessor getAccessor(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex <= 0 || columnIndex > accessors.length) {
            throw new SQLException(
                    "Column index " + columnIndex + " out of bounds (" + accessors.length + " columns available)");
        }
        return accessors[columnIndex - 1];
    }

    private void updateWasNull(QueryJDBCAccessor accessor) throws SQLException {
        wasNull = accessor.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getString();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getBoolean();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getByte();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getShort();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getInt();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getLong();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getFloat();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getDouble();
        updateWasNull(accessor);
        return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getBigDecimal(scale);
        updateWasNull(accessor);
        return result;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getBigDecimal();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getBytes();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getDate(cal);
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getTime(cal);
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getTimestamp(cal);
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        // For ARRAY columns, dispatch to getArray() to return DataCloudArray
        // (matching the behavior of the old AvaticaResultSet type dispatch)
        if (metadata.getColumnType(columnIndex) == Types.ARRAY) {
            return getArray(columnIndex);
        }
        val accessor = getAccessor(columnIndex);
        val result = accessor.getObject();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getObject(map);
        updateWasNull(accessor);
        return result;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getObject(type);
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getArray();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getAsciiStream();
        updateWasNull(accessor);
        return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getUnicodeStream();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getBinaryStream();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getCharacterStream();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getRef();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getBlob();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getClob();
        updateWasNull(accessor);
        return result;
    }

    public Struct getStruct(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getStruct is not supported");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getURL();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId is not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getSQLXML();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getNString();
        updateWasNull(accessor);
        return result;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        val accessor = getAccessor(columnIndex);
        val result = accessor.getNCharacterStream();
        updateWasNull(accessor);
        return result;
    }

    // --- Miscellaneous ResultSet methods ---

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // no-op: streaming result set controls its own fetch size
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no-op
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName is not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
}
