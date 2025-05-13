package com.salesforce.datacloud.jdbc.resultset;

import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class SimpleResultSet implements ReadOnlyResultSet, ForwardOnlyResultSet, ResultSetWithPositionalGetters {

    @Override
    public boolean next() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'next'");
    }

    @Override
    public int getRow() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRow'");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMetaData'");
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findColumn'");
    }

    @Override
    public Statement getStatement() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStatement'");
    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isClosed'");
    }

    @Override
    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getHoldability'");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getWarnings'");
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearWarnings'");
    }

    @Override
    public String getCursorName() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCursorName'");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setFetchSize'");
    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFetchSize'");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'unwrap'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isWrapperFor'");
    }

    ////////////////////////////////
    /// Accessors for SQL values

    @Override
    public boolean wasNull() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'wasNull'");
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getString'");
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBoolean'");
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getByte'");
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getShort'");
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getInt'");
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLong'");
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFloat'");
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDouble'");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBigDecimal'");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBigDecimal'");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBytes'");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDate'");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDate'");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTime'");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTime'");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTimestamp'");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTimestamp'");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getObject'");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getObject'");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getObject'");
    }

    ////////////////////////////////
    // Unsupported getters

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving Blobs is not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving Clobs is not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving Ref objects is not supported");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving URLs is not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving row IDs is not supported");
    }
    
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving SQLXML is not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving NStrings is not supported");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving NCharacterStreams is not supported");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving AsciiStreams is not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving UnicodeStreams is not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving BinaryStreams is not supported");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Retrieving CharacterStreams is not supported");
    }
}
