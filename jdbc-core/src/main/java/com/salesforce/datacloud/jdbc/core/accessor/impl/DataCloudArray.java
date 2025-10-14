/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.ArrowToColumnTypeMapper.toColumnType;

import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.FieldVector;

public class DataCloudArray implements Array {

    private final Object[] data;
    private final String baseTypeName;
    private final int baseType;
    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY =
            "Array method is not supported in Data Cloud query";

    public DataCloudArray(FieldVector dataVector, long startOffset, long valuesCount) {
        // Extract data immediately during construction (following PostgreSQL JDBC pattern)
        // This makes the object self-contained and independent of ValueVector lifecycle
        this.data = extractDataFromVector(dataVector, startOffset, valuesCount);
        this.baseTypeName = toColumnType(dataVector.getField()).getType().getName();
        this.baseType = toColumnType(dataVector.getField()).getType().getVendorTypeNumber();
    }

    /**
     * Extracts data from ValueVector immediately to avoid lifecycle dependencies.
     * This follows the same pattern as PostgreSQL JDBC: extract data during construction.
     */
    private Object[] extractDataFromVector(FieldVector dataVector, long startOffset, long valuesCount) {
        // Handle empty array case
        if (valuesCount <= 0) {
            return new Object[0];
        }

        // Validate bounds against actual vector size
        if (startOffset < 0 || startOffset >= dataVector.getValueCount()) {
            throw new ArrayIndexOutOfBoundsException(
                    "Start offset " + startOffset + " is out of bounds for vector size " + dataVector.getValueCount());
        }

        long endOffset = startOffset + valuesCount;
        if (endOffset > dataVector.getValueCount()) {
            throw new ArrayIndexOutOfBoundsException(
                    "End offset " + endOffset + " exceeds vector size " + dataVector.getValueCount());
        }

        // Extract data into a new array
        Object[] result = new Object[LargeMemoryUtil.checkedCastToInt(valuesCount)];
        for (int i = 0; i < valuesCount; i++) {
            long index = startOffset + i;
            result[i] = dataVector.getObject(LargeMemoryUtil.checkedCastToInt(index));
        }
        return result;
    }

    @Override
    public String getBaseTypeName() {
        return this.baseTypeName;
    }

    @Override
    public int getBaseType() {
        return this.baseType;
    }

    @Override
    public Object getArray() throws SQLException {
        return getArray(null);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (map != null) {
            throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
        }
        return this.data;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return getArray(index, count, null);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (map != null) {
            throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
        }
        checkBoundaries(index, count);
        int startIndex = (int) index - 1;
        Object[] result = new Object[count];
        System.arraycopy(this.data, startIndex, result, 0, count);
        return result;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void free() {
        // no-op
    }

    private void checkBoundaries(long index, int count) {
        // JDBC arrays use 1-based indexing: valid range is 1 <= index <= array.length
        // Special case: getArray(1, 0) on empty array is valid (returns empty array)

        long dataIndex = index - 1; // Convert to 0-based for internal logic

        // Validate JDBC index bounds
        if (dataIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(
                    "Index " + index + " is invalid (JDBC arrays use 1-based indexing)");
        }
        if (dataIndex > this.data.length) {
            throw new ArrayIndexOutOfBoundsException("Index " + index + " is out of bounds for array size "
                    + this.data.length + " (JDBC arrays use 1-based indexing)");
        }

        // Validate count bounds (only check when count > 0)
        if (count > 0 && (dataIndex + count) > this.data.length) {
            throw new ArrayIndexOutOfBoundsException("Index " + index + " + count " + count + " exceeds array size "
                    + this.data.length + " (JDBC arrays use 1-based indexing)");
        }
    }
}
