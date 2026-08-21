/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.protocol.data.ArrowUtils.toArrowByteArray;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCDateFromDateAndCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromTimeAndCalendar;

import com.google.protobuf.ByteString;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.HyperTypeKind;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterBinding;
import com.salesforce.datacloud.jdbc.util.QueryTimeout;
import com.salesforce.datacloud.jdbc.util.SqlErrorCodes;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
import lombok.val;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryParameterArrow;

/** Shared prepared-statement execution and value conversion for positional and named parameters. */
abstract class DataCloudPreparedStatementBase<P> extends DataCloudStatement {
    private final String sql;
    private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    private boolean fetchingMetadata;

    DataCloudPreparedStatementBase(DataCloudConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }

    /** Stores one typed parameter using the parameter representation of the concrete statement. */
    protected abstract void bindParameter(P parameter, HyperType type, Object value) throws SQLException;

    protected abstract ProvidedParameters getProvidedParameters();

    public abstract void clearParameters();

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Per the JDBC specification this method cannot be called on a PreparedStatement, use "
                + getClass().getSimpleName()
                + "::executeQuery() instead.");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Per the JDBC specification this method cannot be called on a PreparedStatement, use "
                + getClass().getSimpleName()
                + "::execute() instead.");
    }

    @Override
    public DataCloudStatement executeAsyncQuery(String sql) throws SQLException {
        throw new SQLException("This method cannot be called on a prepared statement, use "
                + getClass().getSimpleName()
                + "::executeAsyncQuery() instead.");
    }

    @Override
    protected QueryParam.Builder getQueryParamBuilder(
            String sql, QueryTimeout queryTimeout, QueryParam.TransferMode transferMode) throws SQLException {
        val builder = super.getQueryParamBuilder(sql, queryTimeout, transferMode);

        final byte[] encodedRow;
        ProvidedParameters providedParameters = getProvidedParameters();
        try {
            encodedRow = toArrowByteArray(providedParameters.getEntries(), calendar);
        } catch (IOException e) {
            throw new SQLException("Failed to encode parameters on prepared statement", e);
        } catch (RuntimeException e) {
            throw new SQLException("Failed to encode parameters on prepared statement: " + e.getMessage(), "HY000", e);
        }

        if (fetchingMetadata) {
            builder.setQueryRowLimit(0);
        }

        return builder.setParamStyle(providedParameters.getStyle())
                .setArrowParameters(QueryParameterArrow.newBuilder()
                        .setData(ByteString.copyFrom(encodedRow))
                        .build());
    }

    public boolean executeAsyncQuery() throws SQLException {
        super.executeAsyncQueryInternal(sql);
        return true;
    }

    public boolean execute() throws SQLException {
        resultSet = executeQuery();
        return true;
    }

    public ResultSet executeQuery() throws SQLException {
        resultSet = super.executeQuery(sql);
        return resultSet;
    }

    public int executeUpdate() throws SQLException {
        throw new SQLException(NOT_SUPPORTED_IN_DATACLOUD_QUERY, SqlErrorCodes.FEATURE_NOT_SUPPORTED);
    }

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

    protected final void setNullParameter(P parameter, int sqlType) throws SQLException {
        HyperType type = hyperTypeForJdbcCode(sqlType);
        if (!isBindableParameterType(type.getKind())) {
            throw new SQLFeatureNotSupportedException(
                    "Binding null parameters of JDBC type " + sqlType + " is not supported", "HYC00");
        }
        setParameter(parameter, type, null);
    }

    protected final void setBooleanParameter(P parameter, boolean value) throws SQLException {
        setParameter(parameter, HyperType.bool(false), value);
    }

    protected final void setByteParameter(P parameter, byte value) throws SQLException {
        setParameter(parameter, HyperType.int8(false), value);
    }

    protected final void setShortParameter(P parameter, short value) throws SQLException {
        setParameter(parameter, HyperType.int16(false), value);
    }

    protected final void setIntParameter(P parameter, int value) throws SQLException {
        setParameter(parameter, HyperType.int32(false), value);
    }

    protected final void setLongParameter(P parameter, long value) throws SQLException {
        setParameter(parameter, HyperType.int64(false), value);
    }

    protected final void setFloatParameter(P parameter, float value) throws SQLException {
        setParameter(parameter, HyperType.float4(false), value);
    }

    protected final void setDoubleParameter(P parameter, double value) throws SQLException {
        setParameter(parameter, HyperType.float8(false), value);
    }

    protected final void setBigDecimalParameter(P parameter, BigDecimal value) throws SQLException {
        HyperType type = value == null
                ? HyperType.decimal(0, 0, true)
                : HyperType.decimal(value.precision(), value.scale(), true);
        setParameter(parameter, type, value);
    }

    protected final void setStringParameter(P parameter, String value) throws SQLException {
        setParameter(parameter, HyperType.varcharUnlimited(true), value);
    }

    protected final void setDateParameter(P parameter, Date value) throws SQLException {
        setParameter(parameter, HyperType.date(true), value);
    }

    protected final void setTimeParameter(P parameter, Time value) throws SQLException {
        setParameter(parameter, HyperType.time(true), value);
    }

    protected final void setTimestampParameter(P parameter, Timestamp value) throws SQLException {
        setParameter(parameter, HyperType.timestamp(true), value == null ? null : toWallClockAsUtc(value, null));
    }

    protected final void setObjectParameter(P parameter, Object value, int targetSqlType) throws SQLException {
        if (value == null) {
            setNullParameter(parameter, Types.NULL);
            return;
        }
        if (targetSqlType == Types.TIMESTAMP) {
            if (value instanceof Timestamp) {
                setParameter(parameter, HyperType.timestamp(true), toWallClockAsUtc((Timestamp) value, null));
                return;
            }
            if (value instanceof LocalDateTime) {
                LocalDateTime dateTime = (LocalDateTime) value;
                setParameter(parameter, HyperType.timestamp(true), Timestamp.from(dateTime.toInstant(ZoneOffset.UTC)));
                return;
            }
        }
        setParameter(parameter, hyperTypeForJdbcCode(targetSqlType), value);
    }

    protected final void setObjectParameter(P parameter, Object value) throws SQLException {
        if (value == null) {
            setNullParameter(parameter, Types.NULL);
            return;
        }

        Class<?> valueClass = value.getClass();
        if (valueClass == String.class) {
            setStringParameter(parameter, (String) value);
        } else if (valueClass == BigDecimal.class) {
            setBigDecimalParameter(parameter, (BigDecimal) value);
        } else if (valueClass == Short.class) {
            setShortParameter(parameter, (Short) value);
        } else if (valueClass == Integer.class) {
            setIntParameter(parameter, (Integer) value);
        } else if (valueClass == Long.class) {
            setLongParameter(parameter, (Long) value);
        } else if (valueClass == Float.class) {
            setFloatParameter(parameter, (Float) value);
        } else if (valueClass == Double.class) {
            setDoubleParameter(parameter, (Double) value);
        } else if (valueClass == Date.class) {
            setDateParameter(parameter, (Date) value);
        } else if (valueClass == Time.class) {
            setTimeParameter(parameter, (Time) value);
        } else if (valueClass == Timestamp.class) {
            setTimestampParameter(parameter, (Timestamp) value);
        } else if (valueClass == Boolean.class) {
            setBooleanParameter(parameter, (Boolean) value);
        } else if (valueClass == LocalDateTime.class) {
            setObjectParameter(parameter, value, Types.TIMESTAMP);
        } else if (valueClass == OffsetDateTime.class) {
            setObjectParameter(
                    parameter, Timestamp.from(((OffsetDateTime) value).toInstant()), Types.TIMESTAMP_WITH_TIMEZONE);
        } else if (valueClass == ZonedDateTime.class) {
            setObjectParameter(
                    parameter, Timestamp.from(((ZonedDateTime) value).toInstant()), Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            String message = "Object type not supported for: " + valueClass.getSimpleName() + " (value: " + value + ")";
            throw new SQLFeatureNotSupportedException(message);
        }
    }

    protected final void setDateParameter(P parameter, Date value, Calendar calendar) throws SQLException {
        setParameter(
                parameter, HyperType.date(true), value == null ? null : getUTCDateFromDateAndCalendar(value, calendar));
    }

    protected final void setTimeParameter(P parameter, Time value, Calendar calendar) throws SQLException {
        setParameter(
                parameter, HyperType.time(true), value == null ? null : getUTCTimeFromTimeAndCalendar(value, calendar));
    }

    protected final void setTimestampParameter(P parameter, Timestamp value, Calendar calendar) throws SQLException {
        setParameter(parameter, HyperType.timestamp(true), value == null ? null : toWallClockAsUtc(value, calendar));
    }

    private void setParameter(P parameter, HyperType type, Object value) throws SQLException {
        bindParameter(parameter, type, value);
    }

    private static HyperType hyperTypeForJdbcCode(int sqlType) throws SQLException {
        try {
            return com.salesforce.datacloud.jdbc.core.types.HyperTypes.fromJdbcTypeCode(sqlType, true);
        } catch (IllegalArgumentException ex) {
            throw new SQLException("Unsupported JDBC type code: " + sqlType, "HYC00", ex);
        }
    }

    private static boolean isBindableParameterType(HyperTypeKind kind) {
        switch (kind) {
            case BOOL:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT4:
            case FLOAT8:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case NULL:
                return true;
            default:
                return false;
        }
    }

    private static Timestamp toWallClockAsUtc(Timestamp timestamp, Calendar calendar) {
        // Arrow carries the wall-clock digits as a UTC instant for Hyper's naive TIMESTAMP type.
        ZoneId zone = calendar != null ? calendar.getTimeZone().toZoneId() : ZoneId.systemDefault();
        LocalDateTime wallClock = LocalDateTime.ofInstant(timestamp.toInstant(), zone);
        return Timestamp.from(wallClock.toInstant(ZoneOffset.UTC));
    }

    static final class ProvidedParameters {
        private final QueryParam.ParameterStyle style;
        private final Iterable<? extends Map.Entry<String, ParameterBinding>> entries;

        ProvidedParameters(
                QueryParam.ParameterStyle style, Iterable<? extends Map.Entry<String, ParameterBinding>> entries) {
            this.style = style;
            this.entries = entries;
        }

        QueryParam.ParameterStyle getStyle() {
            return style;
        }

        Iterable<? extends Map.Entry<String, ParameterBinding>> getEntries() {
            return entries;
        }
    }
}
