/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Mixin for a forward-only result set.
 *
 * Overwrites all methods that are not supported by forward-only result sets.
 * Used as a mixin to keep the boilerplate out of the actual result set implementations.
 */
interface ForwardOnlyResultSet extends ResultSet {
    @Override
    public default int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public default int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public default void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Forward only result sets only support forward fetching");
        }
    }

    @Override
    public default boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isBeforeFirst()");
    }

    @Override
    public default boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isAfterLast()");
    }

    @Override
    public default boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isFirst()");
    }

    @Override
    public default boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isLast()");
    }

    @Override
    public default boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support first()");
    }

    @Override
    public default boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support last()");
    }

    @Override
    public default boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support absolute()");
    }

    @Override
    public default boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support relative()");
    }

    @Override
    public default boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support previous()");
    }

    @Override
    public default void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support beforeFirst()");
    }

    @Override
    public default void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support afterLast()");
    }
}
