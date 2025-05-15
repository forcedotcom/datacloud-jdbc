package com.salesforce.datacloud.jdbc.resultset;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * Accessor functions used to read column values from a {@link SimpleResultSet}.
 * 
 * This interface is optimized for performance, and hence avoids the use of boxed types as much as possible.
 */
public interface ColumnAccessor<ConcreteResultSet> {
    default public Boolean getBoolean(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    /// Get the value of the column as an integer. Used for `getShort`, `getInt`, and `getLong`.
    default public OptionalLong	getAnyInteger(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public BigDecimal getBigDecimal(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public OptionalDouble getAnyFloatingPoint(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public String getString(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public byte[] getBytes(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public Date getDate(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public Time getTime(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public Timestamp getTimestamp(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
    default public Array getArray(ConcreteResultSet resultSet) throws SQLException { throw new UnsupportedOperationException(); }
}
