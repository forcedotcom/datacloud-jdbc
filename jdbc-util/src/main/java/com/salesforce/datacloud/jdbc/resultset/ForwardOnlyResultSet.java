package com.salesforce.datacloud.jdbc.resultset;

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
    default public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    default public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    default public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Forward only result sets only support forward fetching");
        }
    }

    @Override
    default public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isBeforeFirst()");
    }

    @Override
    default public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isAfterLast()");
    }

    @Override
    default public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isFirst()");
    }

    @Override
    default public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support isLast()");
    }

    @Override
    default public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support first()");
    }

    @Override
    default public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support last()");
    }

    @Override
    default public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support absolute()");
    }

    @Override
    default public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support relative()");
    }

    @Override
    default public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support previous()");
    }

    @Override
    default public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support beforeFirst()");
    }

    @Override
    default public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Forward only result sets do not support afterLast()");
    }    
}
