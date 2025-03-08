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
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCDateFromDateAndCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromTimeAndCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimestampFromTimestampAndCalendar;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.core.listener.AsyncQueryStatusListener;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.ArrowUtils;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryParameterArrow;

@Slf4j
public class DataCloudPreparedStatement extends DataCloudStatement implements PreparedStatement {
    private String sql;
    private final ParameterManager parameterManager;
    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    DataCloudPreparedStatement(DataCloudConnection connection, ParameterManager parameterManager) {
        super(connection);
        this.parameterManager = parameterManager;
    }

    DataCloudPreparedStatement(DataCloudConnection connection, String sql, ParameterManager parameterManager) {
        super(connection);
        this.sql = sql;
        this.parameterManager = parameterManager;
    }

    private <T> void setParameter(int parameterIndex, int sqlType, T value) throws SQLException {
        try {
            parameterManager.setParameter(parameterIndex, sqlType, value);
        } catch (SQLException e) {
            throw new DataCloudJDBCException(e);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new DataCloudJDBCException(
                "Per the JDBC specification this method cannot be called on a PreparedStatement, use DataCloudPreparedStatement::executeQuery() instead.");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new DataCloudJDBCException(
                "Per the JDBC specification this method cannot be called on a PreparedStatement, use DataCloudPreparedStatement::execute() instead.");
    }

    @Override
    @SneakyThrows
    protected HyperGrpcClientExecutor getQueryExecutor() {
        final byte[] encodedRow;
        try {
            encodedRow = ArrowUtils.toArrowByteArray(parameterManager.getParameters(), calendar);
        } catch (IOException e) {
            throw new DataCloudJDBCException("Failed to encode parameters on prepared statement", e);
        }

        val preparedQueryParams = QueryParam.newBuilder()
                .setParamStyle(QueryParam.ParameterStyle.QUESTION_MARK)
                .setArrowParameters(QueryParameterArrow.newBuilder()
                        .setData(ByteString.copyFrom(encodedRow))
                        .build())
                .build();

        return getQueryExecutor(preparedQueryParams);
    }

    @Override
    public boolean execute() throws SQLException {
        val client = getQueryExecutor();
        listener = AsyncQueryStatusListener.of(sql, client);
        return true;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        val client = getQueryExecutor();
        val timeout = Duration.ofSeconds(getQueryTimeout());

        val useSync = optional(this.dataCloudConnection.getProperties(), Constants.FORCE_SYNC)
                .map(Boolean::parseBoolean)
                .orElse(false);
        resultSet = useSync ? executeSyncQuery(sql, client) : executeAdaptiveQuery(sql, client, timeout);
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, sqlType, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, Types.BOOLEAN, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, Types.TINYINT, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, Types.SMALLINT, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, Types.INTEGER, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, Types.BIGINT, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, Types.FLOAT, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, Types.DOUBLE, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, Types.DECIMAL, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, Types.VARCHAR, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, Types.DATE, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter(parameterIndex, Types.TIME, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, Types.TIMESTAMP, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void clearParameters() {
        parameterManager.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
            return;
        }
        setParameter(parameterIndex, targetSqlType, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
            return;
        }

        TypeHandler handler = TypeHandlers.typeHandlerMap.get(x.getClass());
        if (handler != null) {
            try {
                handler.setParameter(this, parameterIndex, x);
            } catch (SQLException e) {
                throw new DataCloudJDBCException(e);
            }
        } else {
            String message = "Object type not supported for: " + x.getClass().getSimpleName() + " (value: " + x + ")";
            throw new DataCloudJDBCException(new SQLFeatureNotSupportedException(message));
        }
    }

    @Override
    public void addBatch() throws SQLException {
        throw new DataCloudJDBCException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        val utcDate = getUTCDateFromDateAndCalendar(x, cal);
        setParameter(parameterIndex, Types.DATE, utcDate);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        val utcTime = getUTCTimeFromTimeAndCalendar(x, cal);
        setParameter(parameterIndex, Types.TIME, utcTime);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        val utcTimestamp = getUTCTimestampFromTimestampAndCalendar(x, cal);
        setParameter(parameterIndex, Types.TIMESTAMP, utcTimestamp);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new DataCloudJDBCException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public <T> T unwrap(Class<T> iFace) throws SQLException {
        if (iFace.isInstance(this)) {
            return iFace.cast(this);
        }
        throw new DataCloudJDBCException("Cannot unwrap to " + iFace.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iFace) {
        return iFace.isInstance(this);
    }
}

@FunctionalInterface
interface TypeHandler {
    void setParameter(PreparedStatement ps, int parameterIndex, Object value) throws SQLException;
}

@UtilityClass
final class TypeHandlers {
    public static final TypeHandler STRING_HANDLER = (ps, idx, value) -> ps.setString(idx, (String) value);
    public static final TypeHandler BIGDECIMAL_HANDLER = (ps, idx, value) -> ps.setBigDecimal(idx, (BigDecimal) value);
    public static final TypeHandler SHORT_HANDLER = (ps, idx, value) -> ps.setShort(idx, (Short) value);
    public static final TypeHandler INTEGER_HANDLER = (ps, idx, value) -> ps.setInt(idx, (Integer) value);
    public static final TypeHandler LONG_HANDLER = (ps, idx, value) -> ps.setLong(idx, (Long) value);
    public static final TypeHandler FLOAT_HANDLER = (ps, idx, value) -> ps.setFloat(idx, (Float) value);
    public static final TypeHandler DOUBLE_HANDLER = (ps, idx, value) -> ps.setDouble(idx, (Double) value);
    public static final TypeHandler DATE_HANDLER = (ps, idx, value) -> ps.setDate(idx, (Date) value);
    public static final TypeHandler TIME_HANDLER = (ps, idx, value) -> ps.setTime(idx, (Time) value);
    public static final TypeHandler TIMESTAMP_HANDLER = (ps, idx, value) -> ps.setTimestamp(idx, (Timestamp) value);
    public static final TypeHandler BOOLEAN_HANDLER = (ps, idx, value) -> ps.setBoolean(idx, (Boolean) value);
    static final Map<Class<?>, TypeHandler> typeHandlerMap = ImmutableMap.ofEntries(
            Maps.immutableEntry(String.class, STRING_HANDLER),
            Maps.immutableEntry(BigDecimal.class, BIGDECIMAL_HANDLER),
            Maps.immutableEntry(Short.class, SHORT_HANDLER),
            Maps.immutableEntry(Integer.class, INTEGER_HANDLER),
            Maps.immutableEntry(Long.class, LONG_HANDLER),
            Maps.immutableEntry(Float.class, FLOAT_HANDLER),
            Maps.immutableEntry(Double.class, DOUBLE_HANDLER),
            Maps.immutableEntry(Date.class, DATE_HANDLER),
            Maps.immutableEntry(Time.class, TIME_HANDLER),
            Maps.immutableEntry(Timestamp.class, TIMESTAMP_HANDLER),
            Maps.immutableEntry(Boolean.class, BOOLEAN_HANDLER));
}
