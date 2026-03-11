/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.resultset;

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
    public default Boolean getBoolean(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }
    /// Get the value of the column as an integer. Used for `getShort`, `getInt`, and `getLong`.
    public default OptionalLong getAnyInteger(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default BigDecimal getBigDecimal(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default OptionalDouble getAnyFloatingPoint(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default String getString(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default byte[] getBytes(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default Date getDate(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default Time getTime(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default Timestamp getTimestamp(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public default Array getArray(ConcreteResultSet resultSet) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
