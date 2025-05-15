package com.salesforce.datacloud.jdbc.resultset;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import com.salesforce.datacloud.jdbc.metadata.SimpleResultSetMetaData;
import com.salesforce.datacloud.jdbc.resultset.ColumnAccessor.ThreeValuedBoolean;

import lombok.AllArgsConstructor;
import lombok.val;
/**
 * A base class for simple result sets.
 * 
 * This class provides a basic implementation of the {@link ResultSet} interface,
 * with support for read-only access and forward-only cursors.
 * 
 * Access to SQL values is provided via {@link ColumnAccessor} instances. This class
 * already takes care of casting from SQL types to the compatible Java types.
 */
@AllArgsConstructor
public abstract class SimpleResultSet<SELF> implements ReadOnlyResultSet, ForwardOnlyResultSet, ResultSetWithPositionalGetters {
    /// The metadata for the result set
    protected final SimpleResultSetMetaData metadata;
    /// The accessor functions for the columns
    protected final ColumnAccessor<SELF>[] accessors;
    /// Was the previously read value null?
    private boolean wasNull = false;

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return metadata.findColumn(columnLabel);
    }

    @Override
    public int getHoldability() throws SQLException {
        // Our result sets are independent of transaction state
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // No-op, since this is just a hint
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // We don't support per-row warnings.
        // Note that `ResultSet.getWarnings()` retrieves per-row warnings.
        // Warnings for the overall query would be attached to the Statement, not the ResultSet.
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // No-op, since we don't support warnings
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName is not supported");
    }

    ////////////////////////////////
    /// Accessors for SQL values

    /// Get the accessor for a column
    private ColumnAccessor<SELF> getAccessor(int columnIndex) throws SQLException {
        if (columnIndex <= 0 || columnIndex >= accessors.length) {
            throw new SQLException("Column index " + columnIndex + " out of bounds (" + accessors.length + " columns available)");
        }
        return accessors[columnIndex - 1];
    }

    @SuppressWarnings("unchecked")
    private SELF getSubclass() {
        return (SELF) this;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        String value = getAccessor(columnIndex).getString(getSubclass());
        wasNull = value == null;
        return value;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Boolean value = getAccessor(columnIndex).getBoolean(getSubclass());
        wasNull = value == null;
        return value == null ? false : value;
    }

    public byte getByte(int columnIndex) throws SQLException {
        long v = getLong(columnIndex);
        if (wasNull) {
            return 0;
        }
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
            throw new SQLException("Column " + getMetaData().getColumnName(columnIndex) + " is out of range for a byte");
        }
        return (byte) v;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        long v = getLong(columnIndex);
        if (wasNull) {
            return 0;
        }
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
            throw new SQLException("Column " + getMetaData().getColumnName(columnIndex) + " is out of range for a short");
        }
        return (short) v;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        long v = getLong(columnIndex);
        if (wasNull) {
            return 0;
        }
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw new SQLException("Column " + getMetaData().getColumnName(columnIndex) + " is out of range for an int");
        }
        return (int) v;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        switch (metadata.getColumn(columnIndex).getType().getType()) {
            case SmallInt:
            case Integer:
            case BigInt: {
                OptionalLong v = getAccessor(columnIndex).getAnyInteger(getSubclass());
                wasNull = v.isEmpty();
                return v.orElse(0L);
            }
            case Float:
            case Double: {
                OptionalDouble d = getAccessor(columnIndex).getAnyFloatingPoint(getSubclass());
                wasNull = d.isEmpty();
                double dv = d.orElse(0.0);
                // The way this condition is written, it will never be true for NaN or Infinity.
                // This is good because we want to throw an exception for those values.
                if (dv >= LONG_MIN_DOUBLE && dv <= LONG_MAX_DOUBLE) {
                    return (long) dv;
                }
                throw new SQLException("Column " + getMetaData().getColumnName(columnIndex) + " is out of range for an integer-like type");
            }
            case Numeric: {
                BigDecimal v = getBigDecimal(columnIndex);
                wasNull = v == null;
                if (wasNull) {
                    return 0L;
                } else {
                    BigInteger i = v.toBigInteger();
                    int gt = i.compareTo(BigInteger.valueOf(Long.MAX_VALUE));
                    int lt = i.compareTo(BigInteger.valueOf(Long.MIN_VALUE));
                    if (gt > 0 || lt < 0) {
                        throw new SQLException("Column " + getMetaData().getColumnName(columnIndex) + " is out of range for an integer-like type");
                    }
                    return v.longValue();
                }
            }
            default:
                throw new SQLException("Unsupported column type for integer-like types: " + metadata.getColumn(columnIndex).getType().toString());
        }
    }

    private static final double LONG_MAX_DOUBLE = StrictMath.nextDown((double) Long.MAX_VALUE);
    private static final double LONG_MIN_DOUBLE = StrictMath.nextUp((double) Long.MIN_VALUE);

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        double v = getDouble(columnIndex);
        if (v < Float.MIN_VALUE || v > Float.MAX_VALUE) {
            throw new SQLException("Column " + getMetaData().getColumnName(columnIndex) + " is out of range for a float");
        }
        return (float) v;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        switch (metadata.getColumn(columnIndex).getType().getType()) {
            case SmallInt:
            case Integer:
            case BigInt: {
                OptionalLong v = getAccessor(columnIndex).getAnyInteger(getSubclass());
                wasNull = v.isEmpty();
                return v.orElse(0L);
            }
            case Float:
            case Double: {
                val v = getAccessor(columnIndex).getAnyFloatingPoint(getSubclass());
                wasNull = v.isEmpty();
                return v.orElse(0.0);
            }
            case Numeric: {
                BigDecimal v = getBigDecimal(columnIndex);
                wasNull = v == null;
                return v == null ? 0.0 : v.doubleValue();
            }
            default:
                throw new SQLException("Unsupported column type for floating-point types: " + metadata.getColumn(columnIndex).getType().toString());
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        switch (metadata.getColumn(columnIndex).getType().getType()) {
            case SmallInt:
            case Integer:
            case BigInt: {
                OptionalLong v = getAccessor(columnIndex).getAnyInteger(getSubclass());
                wasNull = v.isEmpty();
                return v.map(BigDecimal::valueOf).orElse(null);
            }
            // TODO: apparently, PostgreSQL does not support float/decimal conversion. Double-check this with test cases.
            case Numeric: {
                val v = getAccessor(columnIndex).getBigDecimal(getSubclass());
                wasNull = v == null;
                return v;
            }
            default:
                throw new SQLException("Unsupported column type for numeric types: " + metadata.getColumn(columnIndex).getType().toString());
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        val v = getBigDecimal(columnIndex);
        if (wasNull) {
            return null;
        }
        try {
            return v.setScale(scale);
        } catch (ArithmeticException e) {
            throw new SQLException("Bad value for type BigDecimal: " + v);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        val v = getAccessor(columnIndex).getBytes(getSubclass());
        wasNull = v == null;
        return v;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO implement this
        throw new UnsupportedOperationException("Unimplemented method 'getDate'");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTime'");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTimestamp'");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        val v = getAccessor(columnIndex).getArray(getSubclass());
        wasNull = v == null;
        return v;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        switch (metadata.getColumn(columnIndex).getType().getType()) {
            case Bool: {
                val v = getBoolean(columnIndex);
                if (wasNull) {
                    return null;
                }
                return v;
            }
            case SmallInt: {
                val v = getShort(columnIndex);
                if (wasNull) {
                    return null;
                }
                return v;
            }
            case Integer: {
                val v = getInt(columnIndex);
                if (wasNull) {
                    return null;
                }
                return v;
            }
            case BigInt: {
                val v = getLong(columnIndex);
                if (wasNull) {
                    return null;
                }
                return v;
            }
            case Numeric:
                return getBigDecimal(columnIndex);
            case Float: {
                val v = getFloat(columnIndex);
                if (wasNull) {
                    return null;
                }
                return v;
            }
            case Double: {
                val v = getDouble(columnIndex);
                if (wasNull) {
                    return null;
                }
                return v;
            }
            case Char:
            case Varchar:
                return getString(columnIndex);
            case Bytea:
                return getBytes(columnIndex);
            case Date:
                return getDate(columnIndex);
            case Time:
                return getTime(columnIndex);
            case Timestamp:
            case TimestampTZ:
                return getTimestamp(columnIndex);
            case Interval:
                throw new SQLException("Unsupported column type in `getObject`: " + metadata.getColumn(columnIndex).getType().toString());
            case Array:
                return getArray(columnIndex);
        }
        throw new SQLException("Unsupported column type in `getObject`: " + metadata.getColumn(columnIndex).getType().toString());
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        val v = getObject(columnIndex);
        if (v == null) {
            return null;
        }
        if (type.isInstance(v)) {
            return type.cast(v);
        }
        throw new SQLException("Unsupported column type in `getObject`: " + metadata.getColumn(columnIndex).getType().toString());
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty()) {
            return getObject(columnIndex);
        }
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
