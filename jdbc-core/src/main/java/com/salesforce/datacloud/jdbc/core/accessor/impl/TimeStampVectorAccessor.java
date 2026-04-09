/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorGetter.createGetter;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Accessor for naive TIMESTAMP columns (no timezone metadata in Arrow).
 *
 * <p>Naive timestamps store literal date-time values without timezone context. The literal is
 * preserved: {@code getTimestamp()} returns a {@code Timestamp} whose {@code toString()} renders
 * the original wall-clock value in the JVM's default timezone. This matches the behaviour of
 * the PostgreSQL JDBC driver for {@code TIMESTAMP} (without time zone) columns.
 *
 * <p>When a {@link Calendar} parameter is provided to {@code getTimestamp(Calendar)}, the literal
 * is interpreted in that calendar's timezone rather than the JVM default.
 *
 * <p>Supported JDBC 4.2 types via {@code getObject(Class)}:
 * <ul>
 *   <li>{@link Instant} – raw UTC epoch (literal treated as UTC)
 *   <li>{@link OffsetDateTime} – UTC offset
 *   <li>{@link ZonedDateTime} – UTC zone
 *   <li>{@link LocalDateTime} – literal value preserved
 *   <li>{@link Timestamp} – legacy, literal value preserved
 * </ul>
 */
public class TimeStampVectorAccessor extends QueryJDBCAccessor {
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final ZoneId UTC = ZoneId.of("UTC");
    static final String INVALID_UNIT_ERROR_RESPONSE = "Invalid Arrow time unit";

    private final TimeUnit timeUnit;
    private final TimeStampVectorGetter.Holder holder;
    private final TimeStampVectorGetter.Getter getter;

    public TimeStampVectorAccessor(
            TimeStampVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer)
            throws SQLException {
        super(currentRowSupplier, wasNullConsumer);
        this.timeUnit = getTimeUnitForVector(vector);
        this.holder = new TimeStampVectorGetter.Holder();
        this.getter = createGetter(vector);
    }

    /**
     * Returns the raw stored epoch. Hyper encodes the literal wall-clock as a UTC epoch value,
     * so this instant represents the literal treated as UTC.
     */
    Instant getInstant() {
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);

        if (this.wasNull) {
            return null;
        }

        long value = holder.value;

        switch (timeUnit) {
            case NANOSECONDS:
                return Instant.ofEpochSecond(value / 1_000_000_000, value % 1_000_000_000);
            case MICROSECONDS:
                return Instant.ofEpochSecond(value / 1_000_000, (value % 1_000_000) * 1_000);
            case MILLISECONDS:
                return Instant.ofEpochMilli(value);
            case SECONDS:
                return Instant.ofEpochSecond(value);
            default:
                throw new IllegalStateException(INVALID_UNIT_ERROR_RESPONSE);
        }
    }

    private OffsetDateTime getOffsetDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }

        if (calendar != null) {
            return OffsetDateTime.ofInstant(instant, calendar.getTimeZone().toZoneId());
        }

        return OffsetDateTime.ofInstant(instant, UTC);
    }

    private LocalDateTime getLocalDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }
        return LocalDateTime.ofInstant(instant, UTC);
    }

    private ZonedDateTime getZonedDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }

        if (calendar != null) {
            return ZonedDateTime.ofInstant(instant, calendar.getTimeZone().toZoneId());
        }

        return ZonedDateTime.ofInstant(instant, UTC);
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) {
        LocalDateTime localDateTime = getLocalDateTime(calendar);
        if (localDateTime == null) {
            return null;
        }

        ZoneId zone = calendar != null ? calendar.getTimeZone().toZoneId() : ZoneId.systemDefault();
        return Timestamp.from(localDateTime.atZone(zone).toInstant());
    }

    @Override
    public Date getDate(Calendar calendar) {
        Timestamp ts = getTimestamp(calendar);
        if (ts == null) {
            return null;
        }
        return new Date(ts.getTime());
    }

    @Override
    public Time getTime(Calendar calendar) {
        Timestamp ts = getTimestamp(calendar);
        if (ts == null) {
            return null;
        }
        return new Time(ts.getTime());
    }

    @Override
    public String getString() {
        LocalDateTime ldt = getLocalDateTime(null);
        if (ldt == null) {
            return null;
        }
        return ldt.format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
    }

    @Override
    public Object getObject() {
        return getTimestamp(null);
    }

    @Override
    public Class<?> getObjectClass() {
        return Timestamp.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("type parameter must not be null", "22023");
        }

        if (type == Instant.class) {
            return (T) getInstant();
        }
        if (type == OffsetDateTime.class) {
            return (T) getOffsetDateTime(null);
        }
        if (type == ZonedDateTime.class) {
            return (T) getZonedDateTime(null);
        }
        if (type == LocalDateTime.class) {
            return (T) getLocalDateTime(null);
        }
        if (type == Timestamp.class) {
            return (T) getTimestamp(null);
        }
        if (type == Date.class) {
            return (T) getDate(null);
        }
        if (type == Time.class) {
            return (T) getTime(null);
        }
        if (type == String.class) {
            return (T) getString();
        }

        throw new SQLFeatureNotSupportedException("Unsupported conversion type: " + type.getName());
    }

    static TimeUnit getTimeUnitForVector(TimeStampVector vector) throws SQLException {
        ArrowType.Timestamp arrowType =
                (ArrowType.Timestamp) vector.getField().getFieldType().getType();

        switch (arrowType.getUnit()) {
            case NANOSECOND:
                return TimeUnit.NANOSECONDS;
            case MICROSECOND:
                return TimeUnit.MICROSECONDS;
            case MILLISECOND:
                return TimeUnit.MILLISECONDS;
            case SECOND:
                return TimeUnit.SECONDS;
            default:
                val rootCauseException = new UnsupportedOperationException(INVALID_UNIT_ERROR_RESPONSE);
                throw new SQLException(INVALID_UNIT_ERROR_RESPONSE, "22007", rootCauseException);
        }
    }
}
