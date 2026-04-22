/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.protocol.data.ArrowUtils.toArrowByteArray;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCDateFromDateAndCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromTimeAndCalendar;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterAccumulator;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
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
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryParameterArrow;

@Slf4j
public class DataCloudPreparedStatement extends DataCloudStatement implements PreparedStatement {
    private String sql;
    private final ParameterAccumulator parameterManager;
    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    // True if we are currently fetching metadata from the server, this influences the query param generation
    // to not return any data.
    private boolean fetchingMetadata = false;

    DataCloudPreparedStatement(DataCloudConnection connection, ParameterAccumulator parameterManager) {
        super(connection);
        this.parameterManager = parameterManager;
    }

    DataCloudPreparedStatement(DataCloudConnection connection, String sql, ParameterAccumulator parameterManager) {
        super(connection);
        this.sql = sql;
        this.parameterManager = parameterManager;
    }

    private <T> void setParameter(int parameterIndex, HyperType type, T value) throws SQLException {
        try {
            parameterManager.setParameter(parameterIndex, type, value);
        } catch (IllegalArgumentException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException(
                "Per the JDBC specification this method cannot be called on a PreparedStatement, use DataCloudPreparedStatement::executeQuery() instead.");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException(
                "Per the JDBC specification this method cannot be called on a PreparedStatement, use DataCloudPreparedStatement::execute() instead.");
    }

    @Override
    protected QueryParam.Builder getQueryParamBuilder(
            String sql, QueryTimeout queryTimeout, QueryParam.TransferMode transferMode) throws SQLException {
        val builder = super.getQueryParamBuilder(sql, queryTimeout, transferMode);

        final byte[] encodedRow;
        try {
            encodedRow = toArrowByteArray(parameterManager.getParameters(), calendar);
        } catch (IOException e) {
            throw new SQLException("Failed to encode parameters on prepared statement", e);
        } catch (IllegalArgumentException e) {
            // Thrown when a parameter is bound with a HyperType we cannot currently encode as
            // Arrow (e.g. INTERVAL, JSON). Surface as a JDBC spec-compliant SQLException.
            throw new SQLException("Failed to encode parameters on prepared statement: " + e.getMessage(), "HY000", e);
        }

        if (fetchingMetadata) {
            // Submit the query as metadata only query, with limit 0 Hyper will skip execution.
            builder.setQueryRowLimit(0);
        }

        return builder.setParamStyle(QueryParam.ParameterStyle.QUESTION_MARK)
                .setArrowParameters(QueryParameterArrow.newBuilder()
                        .setData(ByteString.copyFrom(encodedRow))
                        .build());
    }

    public boolean executeAsyncQuery() throws SQLException {
        super.executeAsyncQueryInternal(sql);
        return true;
    }

    @Override
    public boolean execute() throws SQLException {
        resultSet = executeQuery();
        return true;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        resultSet = super.executeQuery(sql);
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, hyperTypeForJdbcCode(sqlType), null);
    }

    private static HyperType hyperTypeForJdbcCode(int sqlType) throws SQLException {
        try {
            return com.salesforce.datacloud.jdbc.core.types.HyperTypes.fromJdbcTypeCode(sqlType, true);
        } catch (IllegalArgumentException ex) {
            throw new SQLException("Unsupported JDBC type code: " + sqlType, "HYC00", ex);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, HyperType.bool(false), x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, HyperType.int8(false), x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, HyperType.int16(false), x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, HyperType.int32(false), x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, HyperType.int64(false), x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, HyperType.float8(false), x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, HyperType.float8(false), x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        HyperType type = x != null ? HyperType.decimal(x.precision(), x.scale(), true) : HyperType.decimal(0, 0, true);
        setParameter(parameterIndex, type, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, HyperType.varcharUnlimited(true), x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, HyperType.date(true), x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter(parameterIndex, HyperType.time(true), x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, HyperType.timestamp(true), toWallClockAsUtc(x, null));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
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
        // TIMESTAMP (naive): apply wall-clock normalization for legacy Timestamp,
        // or store LocalDateTime digits directly.
        if (targetSqlType == Types.TIMESTAMP) {
            if (x instanceof Timestamp) {
                setParameter(parameterIndex, HyperType.timestamp(true), toWallClockAsUtc((Timestamp) x, null));
                return;
            }
            if (x instanceof LocalDateTime) {
                LocalDateTime ldt = (LocalDateTime) x;
                // Encode LDT digits as if they were in UTC — no JVM-TZ shift applied.
                setParameter(parameterIndex, HyperType.timestamp(true), Timestamp.from(ldt.toInstant(ZoneOffset.UTC)));
                return;
            }
        }
        setParameter(parameterIndex, hyperTypeForJdbcCode(targetSqlType), x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
            return;
        }

        TypeHandler handler = TypeHandlers.typeHandlerMap.get(x.getClass());
        if (handler != null) {
            handler.setParameter(this, parameterIndex, x);
        } else {
            String message = "Object type not supported for: " + x.getClass().getSimpleName() + " (value: " + x + ")";
            throw new SQLFeatureNotSupportedException(message);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException(BATCH_EXECUTION_IS_NOT_SUPPORTED, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if ((resultSet != null) && !resultSet.isClosed()) {
            return resultSet.getMetaData();
        }
        try {
            fetchingMetadata = true;
            val result = super.executeQuery(sql);
            val metadata = result.getMetaData();
            result.close();
            return metadata;
        } finally {
            fetchingMetadata = false;
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        val utcDate = getUTCDateFromDateAndCalendar(x, cal);
        setParameter(parameterIndex, HyperType.date(true), utcDate);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        val utcTime = getUTCTimeFromTimeAndCalendar(x, cal);
        setParameter(parameterIndex, HyperType.time(true), utcTime);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParameter(parameterIndex, HyperType.timestamp(true), toWallClockAsUtc(x, cal));
    }

    /**
     * Converts a Timestamp to a naive-UTC representation for sending as a naive TIMESTAMP parameter.
     *
     * <p>Per JDBC spec, {@code setTimestamp(Calendar)} stores the wall-clock value of the Timestamp
     * in the Calendar's timezone (or JVM default if null) as the literal. For example, if the
     * Timestamp represents 14:30 UTC and the Calendar is Asia/Tokyo (UTC+9), the stored literal is
     * "23:30" (the Tokyo wall-clock at that instant).
     *
     * <p>We encode this by extracting the wall-clock in the effective timezone, then re-encoding
     * it as if that wall-clock were in UTC — so the Arrow epoch value carries the wall-clock digits.
     * Hyper receives a naive {@code TimeStampMicroVector} and stores the literal directly.
     */
    private static Timestamp toWallClockAsUtc(Timestamp ts, Calendar cal) {
        ZoneId zone = cal != null ? cal.getTimeZone().toZoneId() : ZoneId.systemDefault();
        LocalDateTime wallClock = LocalDateTime.ofInstant(ts.toInstant(), zone);
        return Timestamp.from(wallClock.toInstant(ZoneOffset.UTC));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

    @Override
    public <T> T unwrap(Class<T> iFace) throws SQLException {
        if (iFace.isInstance(this)) {
            return iFace.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iFace.getName());
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

    // JDBC 4.2 java.time handlers — mapped per JDBC spec Table B-4:
    //   LocalDateTime    → TIMESTAMP               (wall-clock digits, no TZ shift)
    //   OffsetDateTime   → TIMESTAMP_WITH_TIMEZONE (UTC epoch; recommended write path for TIMESTAMPTZ)
    //   ZonedDateTime    → TIMESTAMP_WITH_TIMEZONE (UTC epoch)
    public static final TypeHandler LOCAL_DATE_TIME_HANDLER =
            (ps, idx, value) -> ps.setObject(idx, value, Types.TIMESTAMP);
    public static final TypeHandler OFFSET_DATE_TIME_HANDLER = (ps, idx, value) ->
            ps.setObject(idx, Timestamp.from(((OffsetDateTime) value).toInstant()), Types.TIMESTAMP_WITH_TIMEZONE);
    public static final TypeHandler ZONED_DATE_TIME_HANDLER = (ps, idx, value) ->
            ps.setObject(idx, Timestamp.from(((ZonedDateTime) value).toInstant()), Types.TIMESTAMP_WITH_TIMEZONE);

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
            Maps.immutableEntry(Boolean.class, BOOLEAN_HANDLER),
            Maps.immutableEntry(LocalDateTime.class, LOCAL_DATE_TIME_HANDLER),
            Maps.immutableEntry(OffsetDateTime.class, OFFSET_DATE_TIME_HANDLER),
            Maps.immutableEntry(ZonedDateTime.class, ZONED_DATE_TIME_HANDLER));

    private TypeHandlers() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
