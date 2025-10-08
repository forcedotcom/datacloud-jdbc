/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.function.IntSupplier;
import org.apache.calcite.avatica.util.Cursor.Accessor;

public abstract class QueryJDBCAccessor implements Accessor {
    private final IntSupplier currentRowSupplier;
    protected boolean wasNull;
    protected QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer;

    protected QueryJDBCAccessor(
            IntSupplier currentRowSupplier, QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer) {
        this.currentRowSupplier = currentRowSupplier;
        this.wasNullConsumer = wasNullConsumer;
    }

    protected int getCurrentRow() {
        return currentRowSupplier.getAsInt();
    }

    public abstract Class<?> getObjectClass();

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public boolean getBoolean() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public byte getByte() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public short getShort() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public int getInt() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public long getLong() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public float getFloat() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public double getDouble() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public BigDecimal getBigDecimal() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public byte[] getBytes() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public InputStream getUnicodeStream() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Object getObject() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Ref getRef() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Blob getBlob() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Clob getClob() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Array getArray() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Struct getStruct() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Date getDate(Calendar calendar) throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Time getTime(Calendar calendar) throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public URL getURL() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public NClob getNClob() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public SQLXML getSQLXML() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public String getNString() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public Reader getNCharacterStream() throws SQLException {
        throw getOperationNotSupported(this.getClass());
    }

    @Override
    public <T> T getObject(Class<T> aClass) {
        return null;
    }

    private static SQLException getOperationNotSupported(final Class<?> type) {
        return new SQLFeatureNotSupportedException(
                String.format("Operation not supported for type: %s.", type.getName()));
    }
}
