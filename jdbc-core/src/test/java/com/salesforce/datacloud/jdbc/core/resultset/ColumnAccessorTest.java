/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.resultset;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ColumnAccessorTest {

    @Test
    public void shouldThrowUnsupportedError() {
        ColumnAccessor<SimpleResultSet> columnAccessor = Mockito.mock(ColumnAccessor.class, Mockito.CALLS_REAL_METHODS);
        SimpleResultSet resultSet = Mockito.mock(SimpleResultSet.class, Mockito.CALLS_REAL_METHODS);

        // Test methods from SimpleResultSet that throw SQLFeatureNotSupportedException
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getBoolean(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getAnyInteger(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getBigDecimal(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getAnyFloatingPoint(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getString(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getBytes(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getDate(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getTime(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getTimestamp(resultSet));
        assertThrows(UnsupportedOperationException.class, () -> columnAccessor.getArray(resultSet));
    }
}
