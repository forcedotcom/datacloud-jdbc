/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.types.HyperTypes;
import com.salesforce.datacloud.jdbc.protocol.data.ArrowToHyperTypeMapper;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.HyperTypeKind;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.FieldVector;

public class DataCloudArray implements Array {

    private final HyperType elementType;
    /** A Java array: {@code Object[]}, or a primitive array when built from one on the client. */
    private final Object data;

    protected static final String NOT_SUPPORTED_IN_DATACLOUD_QUERY =
            "Array method is not supported in Data Cloud query";

    public DataCloudArray(FieldVector dataVector, long startOffset, long valuesCount) {
        // Extract data immediately during construction (following PostgreSQL JDBC pattern)
        // This makes the object self-contained and independent of ValueVector lifecycle
        this.elementType = ArrowToHyperTypeMapper.toHyperType(dataVector.getField());
        this.data = extractDataFromVector(dataVector, startOffset, valuesCount);
    }

    /**
     * An array built on the client, e.g. by {@code Connection.createArrayOf}, to bind as a parameter.
     * {@code data} is a Java array (primitive arrays are kept as is); an {@code Object[]} is checked
     * against {@code elementType} so a mismatch fails here rather than at execution.
     */
    public DataCloudArray(HyperType elementType, Object data) throws SQLException {
        if (!data.getClass().isArray()) {
            throw new SQLException("Array data must be a Java array", SqlErrorCodes.FEATURE_NOT_SUPPORTED);
        }
        this.elementType = elementType;
        this.data = data instanceof Object[] ? ((Object[]) data).clone() : data;
        if (data instanceof Object[]) {
            Class<?> expected = expectedClass(elementType.getKind());
            Object[] elements = (Object[]) this.data;
            for (int i = 0; i < elements.length; i++) {
                if (elements[i] != null && !expected.isInstance(elements[i])) {
                    throw new SQLException(
                            "array element " + i + " is a "
                                    + elements[i].getClass().getSimpleName() + " but the array type is "
                                    + HyperTypes.toJdbcTypeName(elementType),
                            "22018");
                }
            }
        }
    }

    public HyperType getElementType() {
        return elementType;
    }

    /** The Java class a client-built element must have; {@code Object} where several are accepted. */
    private static Class<?> expectedClass(HyperTypeKind kind) {
        switch (kind) {
            case BOOL:
                return Boolean.class;
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return Number.class;
            case FLOAT4:
                return Float.class;
            case FLOAT8:
                return Double.class;
            case DECIMAL:
                return BigDecimal.class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case DATE:
                return Date.class;
            case TIME:
                return Time.class;
            case TIMESTAMP_TZ:
                return Timestamp.class;
            default:
                // TIMESTAMP takes Timestamp or LocalDateTime; unbindable kinds fail at bind time
                return Object.class;
        }
    }

    /** Extracts the range as plain Java values (String, Date, Timestamp, ...), so they can be rebound. */
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

        // For VarChar/VarBinary/FixedSizeBinary/TimeStamp inner types, FieldVector.getObject(int)
        // is gated by arrow.enable_null_check_for_get and returns stale buffer bytes for null rows
        // when the flag is off (set by Iceberg). Check validity explicitly via isNull(int), which
        // is not flag-gated.
        Object[] result = new Object[LargeMemoryUtil.checkedCastToInt(valuesCount)];
        for (int i = 0; i < valuesCount; i++) {
            int absIndex = LargeMemoryUtil.checkedCastToInt(startOffset + i);
            result[i] = dataVector.isNull(absIndex) ? null : toJava(dataVector.getObject(absIndex));
        }
        return result;
    }

    /** Arrow's {@code getObject} leaks Arrow/epoch representations; map them to the JDBC Java types. */
    private Object toJava(Object arrowValue) {
        switch (elementType.getKind()) {
            case VARCHAR:
            case CHAR:
                return arrowValue.toString();
            case DATE:
                return Date.valueOf(
                        arrowValue instanceof LocalDateTime
                                ? ((LocalDateTime) arrowValue).toLocalDate()
                                : LocalDate.ofEpochDay(((Number) arrowValue).longValue()));
            case TIME:
                return Time.valueOf(
                        arrowValue instanceof LocalDateTime
                                ? ((LocalDateTime) arrowValue).toLocalTime()
                                : LocalTime.ofNanoOfDay(((Number) arrowValue).longValue() * 1_000));
            case TIMESTAMP:
                return arrowValue instanceof LocalDateTime ? Timestamp.valueOf((LocalDateTime) arrowValue) : arrowValue;
            case TIMESTAMP_TZ:
                return arrowValue instanceof Long ? microsToTimestamp((Long) arrowValue) : arrowValue;
            default:
                return arrowValue;
        }
    }

    private static Timestamp microsToTimestamp(long micros) {
        return Timestamp.from(
                Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000));
    }

    @Override
    public String getBaseTypeName() {
        return HyperTypes.toJdbcTypeName(elementType);
    }

    @Override
    public int getBaseType() {
        return HyperTypes.toJdbcTypeCode(elementType);
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
        Object result = java.lang.reflect.Array.newInstance(data.getClass().getComponentType(), count);
        System.arraycopy(this.data, (int) index - 1, result, 0, count);
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

        int length = java.lang.reflect.Array.getLength(data);
        long dataIndex = index - 1; // Convert to 0-based for internal logic

        // Validate JDBC index bounds
        if (dataIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(
                    "Index " + index + " is invalid (JDBC arrays use 1-based indexing)");
        }
        if (dataIndex > length) {
            throw new ArrayIndexOutOfBoundsException("Index " + index + " is out of bounds for array size " + length
                    + " (JDBC arrays use 1-based indexing)");
        }

        // Validate count bounds (only check when count > 0)
        if (count > 0 && (dataIndex + count) > length) {
            throw new ArrayIndexOutOfBoundsException("Index " + index + " + count " + count + " exceeds array size "
                    + length + " (JDBC arrays use 1-based indexing)");
        }
    }
}
