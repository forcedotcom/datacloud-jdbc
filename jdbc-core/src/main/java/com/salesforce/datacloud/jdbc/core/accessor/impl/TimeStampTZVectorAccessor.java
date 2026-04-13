/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorAccessor.INVALID_UNIT_ERROR_RESPONSE;
import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorAccessor.getTimeUnitForVector;
import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorGetter.createGetter;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
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
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Accessor for TIMESTAMPTZ columns (with timezone metadata in Arrow).
 *
 * <p>TIMESTAMPTZ values are true UTC instants. The Arrow metadata always contains
 * a timezone string (e.g. "UTC"). This accessor applies timezone conversion when
 * returning values.
 *
 * <p>Timezone Precedence (highest to lowest):
 * 1. Calendar parameter (per-call setting)
 * 2. Arrow metadata timezone (from column definition)
 * 3. Session timezone (query setting time_zone)
 * 4. System default
 *
 * <p>Supported JDBC 4.2 types via getObject(Class):
 * - OffsetDateTime (JDBC 4.2 standard type for TIMESTAMPTZ; carries the timezone offset)
 * - ZonedDateTime (with full timezone info)
 * - LocalDateTime (converted to effective timezone)
 * - Timestamp (legacy, uses effective timezone)
 */
public class TimeStampTZVectorAccessor extends QueryJDBCAccessor {
    private static final String TIMESTAMP_WITH_OFFSET_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS xxx";

    private final ZoneId arrowMetadataZone;
    private final TimeUnit timeUnit;
    private final TimeStampVectorGetter.Holder holder;
    private final TimeStampVectorGetter.Getter getter;

    public TimeStampTZVectorAccessor(TimeStampVector vector, IntSupplier currentRowSupplier) throws SQLException {
        super(currentRowSupplier);
        this.arrowMetadataZone = extractArrowMetadataZone(vector);
        this.timeUnit = getTimeUnitForVector(vector);
        this.holder = new TimeStampVectorGetter.Holder();
        this.getter = createGetter(vector);
    }

    private ZoneId resolveEffectiveZoneId(Calendar calendar) {
        if (calendar != null) {
            return calendar.getTimeZone().toZoneId();
        }
        if (arrowMetadataZone != null) {
            return arrowMetadataZone;
        }
        return ZoneId.systemDefault();
    }

    private Instant getInstant() {
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;

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
        return OffsetDateTime.ofInstant(instant, resolveEffectiveZoneId(calendar));
    }

    private LocalDateTime getLocalDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }
        return LocalDateTime.ofInstant(instant, resolveEffectiveZoneId(calendar));
    }

    private ZonedDateTime getZonedDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }
        return ZonedDateTime.ofInstant(instant, resolveEffectiveZoneId(calendar));
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) {
        if (calendar == null) {
            Instant instant = getInstant();
            if (instant == null) {
                return null;
            }
            return Timestamp.from(instant);
        }

        LocalDateTime localDateTime = getLocalDateTime(calendar);
        if (localDateTime == null) {
            return null;
        }
        return Timestamp.from(
                localDateTime.atZone(calendar.getTimeZone().toZoneId()).toInstant());
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
        OffsetDateTime odt = getOffsetDateTime(null);
        if (odt == null) {
            return null;
        }
        return odt.format(DateTimeFormatter.ofPattern(TIMESTAMP_WITH_OFFSET_FORMAT));
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

    private static ZoneId extractArrowMetadataZone(TimeStampVector vector) {
        ArrowType.Timestamp arrowType =
                (ArrowType.Timestamp) vector.getField().getFieldType().getType();

        String timezoneName = arrowType.getTimezone();
        if (timezoneName == null) {
            return null;
        }

        try {
            return ZoneId.of(timezoneName);
        } catch (Exception e) {
            return null;
        }
    }
}
