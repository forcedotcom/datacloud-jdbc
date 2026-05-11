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

public abstract class QueryJDBCAccessor implements DataCloudQueryAccessor {
    private final IntSupplier currentRowSupplier;
    protected boolean wasNull;

    protected QueryJDBCAccessor(IntSupplier currentRowSupplier) {
        this.currentRowSupplier = currentRowSupplier;
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

    /**
     * Default {@code getObject(Class)} implementation.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>{@code null} type → {@code SQLException} with SQLState {@code 22023}.
     *   <li>{@link String} → delegate to {@link #getString()} (JDBC 4.2 Table B-5 mandates this
     *       conversion for every column type).
     *   <li>Otherwise, take the raw object from {@link #getObject()} and return it via
     *       {@code type.isInstance} when it fits — covering identity, supertype and interface
     *       matches.
     *   <li>Anything left throws {@link SQLFeatureNotSupportedException}.
     * </ol>
     *
     * <p>Numeric narrowing (e.g. {@code Integer.class} on a BIGINT column) is intentionally
     * <em>not</em> handled here: the accessor-level primitive getters use unchecked Java casts,
     * so silently truncating a {@code Long.MAX_VALUE} to {@code -1} would be the wrong default.
     * Accessors that need lossless cross-type conversion override this method
     * (see {@link com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorAccessor} for
     * the timestamp → {@link java.time.Instant} / {@link java.time.OffsetDateTime} path).
     */
    @Override
    public <T> T getObject(Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("type parameter must not be null", "22023");
        }

        // String works on every column type per JDBC 4.2 Table B-5; handle it before the
        // raw-object fallback so callers don't depend on getObject() returning a String.
        if (type == String.class) {
            return type.cast(getString());
        }

        // Generic raw-object path: works whenever the column's natural Object representation
        // is already an instance of the requested type (identity, supertype, or interface).
        // Concrete accessors set wasNull inside getObject(); set it defensively here too so
        // a callsite reading a null does not see stale state from an earlier non-null read.
        Object raw = getObject();
        if (raw == null) {
            wasNull = true;
            return null;
        }
        if (type.isInstance(raw)) {
            return type.cast(raw);
        }
        throw new SQLFeatureNotSupportedException(
                "Cannot convert column value of type " + raw.getClass().getName() + " to " + type.getName());
    }

    private static SQLException getOperationNotSupported(final Class<?> type) {
        return new SQLFeatureNotSupportedException(
                String.format("Operation not supported for type: %s.", type.getName()));
    }
}
