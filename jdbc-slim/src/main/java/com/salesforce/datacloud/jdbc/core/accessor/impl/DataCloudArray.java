/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.ArrowUtils.getSQLTypeFromArrowType;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import lombok.val;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

public class DataCloudArray implements Array {

    private final FieldVector dataVector;
    private final long startOffset;
    private final long valuesCount;
    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY =
            "Array method is not supported in Data Cloud query";

    public DataCloudArray(FieldVector dataVector, long startOffset, long valuesCount) {
        this.dataVector = dataVector;
        this.startOffset = startOffset;
        this.valuesCount = valuesCount;
    }

    @Override
    public String getBaseTypeName() {
        val arrowType = this.dataVector.getField().getType();
        val baseType = getSQLTypeFromArrowType(arrowType);
        return JDBCType.valueOf(baseType).getName();
    }

    @Override
    public int getBaseType() {
        val arrowType = this.dataVector.getField().getType();
        return getSQLTypeFromArrowType(arrowType);
    }

    @Override
    public Object getArray() throws SQLException {
        return getArray(null);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (map != null) {
            throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
        }
        return getArrayNoBoundCheck(this.dataVector, this.startOffset, this.valuesCount);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return getArray(index, count, null);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (map != null) {
            throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
        }
        checkBoundaries(index, count);
        val start = LargeMemoryUtil.checkedCastToInt(this.startOffset + index);
        return getArrayNoBoundCheck(this.dataVector, start, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void free() {
        // no-op
    }

    private static Object getArrayNoBoundCheck(ValueVector dataVector, long start, long count) {
        Object[] result = new Object[LargeMemoryUtil.checkedCastToInt(count)];
        for (int i = 0; i < count; i++) {
            result[i] = dataVector.getObject(LargeMemoryUtil.checkedCastToInt(start + i));
        }
        return result;
    }

    private void checkBoundaries(long index, int count) {
        if (index < 0 || index + count > this.startOffset + this.valuesCount) {
            throw new ArrayIndexOutOfBoundsException();
        }
    }
}
