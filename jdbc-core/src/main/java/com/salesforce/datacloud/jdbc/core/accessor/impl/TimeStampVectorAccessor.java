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
 * Accessor for Arrow TimeStamp columns with comprehensive timezone handling.
 *
 * <p>Timezone Precedence (highest to lowest):
 * 1. Calendar parameter (per-call setting)
 * 2. Arrow metadata timezone (from Hyper - TIMESTAMPTZ columns)
 * 3. Session timezone (query setting time_zone)
 * 4. System default
 *
 * <p>Supported JDBC 4.2 types via getObject(Class):
 * - Instant (always UTC)
 * - OffsetDateTime (with timezone offset)
 * - ZonedDateTime (with full timezone info)
 * - LocalDateTime (naive, no timezone)
 * - Timestamp (legacy, uses effective timezone)
 */
public class TimeStampVectorAccessor extends QueryJDBCAccessor {
    // Use 'xxx' (lowercase) to always get numeric offset like +00:00, not 'Z'
    private static final String TIMESTAMP_WITH_OFFSET_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS xxx";
    private static final String TIMESTAMP_WITHOUT_OFFSET_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String INVALID_UNIT_ERROR_RESPONSE = "Invalid Arrow time unit";

    private final ZoneId arrowMetadataZone; // From Arrow metadata (TIMESTAMPTZ)
    private final ZoneId sessionZone; // From query setting time_zone
    private final TimeUnit timeUnit;
    private final TimeStampVectorGetter.Holder holder;
    private final TimeStampVectorGetter.Getter getter;

    /**
     * Constructor with session timezone support.
     *
     * @param vector The timestamp vector
     * @param currentRowSupplier Supplier for current row index
     * @param wasNullConsumer Consumer for wasNull flag
     * @param sessionZone The session timezone (from query setting time_zone)
     * @throws SQLException If vector configuration is invalid
     */
    public TimeStampVectorAccessor(
            TimeStampVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer,
            ZoneId sessionZone)
            throws SQLException {
        super(currentRowSupplier, wasNullConsumer);
        this.arrowMetadataZone = extractArrowMetadataZone(vector);
        this.sessionZone = sessionZone;
        this.timeUnit = getTimeUnitForVector(vector);
        this.holder = new TimeStampVectorGetter.Holder();
        this.getter = createGetter(vector);
    }

    /**
     * Resolves the effective timezone using precedence rules.
     *
     * @param calendar Optional calendar parameter (highest precedence)
     * @return The effective ZoneId to use, never null
     */
    private ZoneId resolveEffectiveZoneId(Calendar calendar) {
        // Priority 1: Calendar parameter (per-call setting)
        if (calendar != null) {
            return calendar.getTimeZone().toZoneId();
        }

        // Priority 2: Arrow metadata timezone (TIMESTAMPTZ)
        if (arrowMetadataZone != null) {
            return arrowMetadataZone;
        }

        // Priority 3: Session timezone (query setting time_zone)
        if (sessionZone != null) {
            return sessionZone;
        }

        // Priority 4: System default
        return ZoneId.systemDefault();
    }

    /**
     * Gets the raw Instant value (always UTC) from the vector.
     * Preserves full precision (nanoseconds).
     *
     * @return The Instant value, or null if the value was SQL NULL
     */
    private Instant getInstant() {
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);

        if (this.wasNull) {
            return null;
        }

        long value = holder.value;

        // Convert to Instant based on time unit, preserving precision
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

    /**
     * Gets OffsetDateTime using the effective timezone.
     *
     * <p>For naive TIMESTAMP (no Arrow metadata timezone):
     * - With no calendar: Use UTC to preserve literal values
     * - With explicit calendar: Apply timezone conversion using that calendar
     *
     * <p>For TIMESTAMPTZ (has Arrow metadata timezone):
     * Apply timezone conversion using the effective timezone.
     *
     * @param calendar Optional calendar to override timezone
     * @return OffsetDateTime with offset, or null if SQL NULL
     */
    private OffsetDateTime getOffsetDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }

        // If calendar parameter is provided, honor it for timezone conversion
        if (calendar != null) {
            ZoneId zone = calendar.getTimeZone().toZoneId();
            return OffsetDateTime.ofInstant(instant, zone);
        }

        // For TIMESTAMPTZ, apply timezone conversion
        if (arrowMetadataZone != null) {
            ZoneId zone = resolveEffectiveZoneId(calendar);
            return OffsetDateTime.ofInstant(instant, zone);
        }

        // For naive TIMESTAMP with no calendar, use UTC to preserve literal values
        return OffsetDateTime.ofInstant(instant, ZoneId.of("UTC"));
    }

    /**
     * Gets LocalDateTime using the effective timezone.
     * Note: LocalDateTime is "naive" - it has no timezone info.
     *
     * <p>For naive TIMESTAMP (no Arrow metadata timezone):
     * - With no calendar: Use UTC to preserve literal values
     * - With explicit calendar: Apply timezone conversion using that calendar
     *
     * <p>For TIMESTAMPTZ (has Arrow metadata timezone):
     * Apply timezone conversion using the effective timezone.
     *
     * @param calendar Optional calendar to override timezone
     * @return LocalDateTime in the effective timezone, or null if SQL NULL
     */
    private LocalDateTime getLocalDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }

        // If calendar parameter is provided, honor it for timezone conversion
        if (calendar != null) {
            ZoneId zone = calendar.getTimeZone().toZoneId();
            return LocalDateTime.ofInstant(instant, zone);
        }

        // For TIMESTAMPTZ (has Arrow metadata), apply timezone conversion
        if (arrowMetadataZone != null) {
            ZoneId zone = resolveEffectiveZoneId(calendar);
            return LocalDateTime.ofInstant(instant, zone);
        }

        // For naive TIMESTAMP with no calendar, Hyper stores as UTC - use UTC to preserve literal values
        return LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    }

    /**
     * Gets ZonedDateTime using the effective timezone.
     *
     * <p>For naive TIMESTAMP (no Arrow metadata timezone):
     * - With no calendar: Use UTC to preserve literal values
     * - With explicit calendar: Apply timezone conversion using that calendar
     *
     * <p>For TIMESTAMPTZ (has Arrow metadata timezone):
     * Apply timezone conversion using the effective timezone.
     *
     * @param calendar Optional calendar to override timezone
     * @return ZonedDateTime with full zone info, or null if SQL NULL
     */
    private ZonedDateTime getZonedDateTime(Calendar calendar) {
        Instant instant = getInstant();
        if (instant == null) {
            return null;
        }

        // If calendar parameter is provided, honor it for timezone conversion
        if (calendar != null) {
            ZoneId zone = calendar.getTimeZone().toZoneId();
            return ZonedDateTime.ofInstant(instant, zone);
        }

        // For TIMESTAMPTZ, apply timezone conversion
        if (arrowMetadataZone != null) {
            ZoneId zone = resolveEffectiveZoneId(calendar);
            return ZonedDateTime.ofInstant(instant, zone);
        }

        // For naive TIMESTAMP with no calendar, use UTC to preserve literal values
        return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
    }

    // ========== JDBC Standard Methods ==========

    @Override
    public Timestamp getTimestamp(Calendar calendar) {
        // For TIMESTAMPTZ with no calendar, return instant directly
        if (calendar == null && arrowMetadataZone != null) {
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

        // For naive TIMESTAMP with no calendar: preserve literal values
        if (calendar == null && arrowMetadataZone == null) {
            // LocalDateTime is in UTC, adjust to system default for literal preservation
            Instant adjustedInstant =
                    localDateTime.atZone(ZoneId.systemDefault()).toInstant();
            return Timestamp.from(adjustedInstant);
        }

        // When explicit calendar is provided (both naive TIMESTAMP and TIMESTAMPTZ)
        return Timestamp.valueOf(localDateTime);
    }

    @Override
    public Date getDate(Calendar calendar) {
        // Use getTimestamp which handles naive TIMESTAMP literal value preservation
        Timestamp ts = getTimestamp(calendar);
        if (ts == null) {
            return null;
        }

        return new Date(ts.getTime());
    }

    @Override
    public Time getTime(Calendar calendar) {
        // Use getTimestamp which handles naive TIMESTAMP literal value preservation
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

        // If Arrow metadata has timezone (TIMESTAMPTZ), show offset
        // If no timezone metadata (TIMESTAMP), hide offset (naive timestamp)
        if (arrowMetadataZone != null) {
            return odt.format(DateTimeFormatter.ofPattern(TIMESTAMP_WITH_OFFSET_FORMAT));
        } else {
            return odt.format(DateTimeFormatter.ofPattern(TIMESTAMP_WITHOUT_OFFSET_FORMAT));
        }
    }

    @Override
    public Object getObject() {
        return getTimestamp(null);
    }

    @Override
    public Class<?> getObjectClass() {
        return Timestamp.class;
    }

    /**
     * JDBC 4.2 typed getObject support.
     * Enables modern java.time types.
     *
     * @param type The target class
     * @param <T> The type parameter
     * @return The converted value, or null if conversion is not supported or value is SQL NULL
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(Class<T> type) {
        if (type == null) {
            return null;
        }

        // JDBC 4.2 java.time types
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

        // Legacy JDBC types
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

        // Unsupported conversion - return null per parent class contract
        return null;
    }

    // ========== Helper Methods ==========

    /**
     * Extracts timezone from Arrow metadata.
     * Returns null for TIMESTAMP (naive), non-null for TIMESTAMPTZ.
     *
     * @param vector The timestamp vector
     * @return ZoneId from metadata, or null if no timezone metadata
     */
    private static ZoneId extractArrowMetadataZone(TimeStampVector vector) {
        ArrowType.Timestamp arrowType =
                (ArrowType.Timestamp) vector.getField().getFieldType().getType();

        String timezoneName = arrowType.getTimezone();
        if (timezoneName == null) {
            return null; // TIMESTAMP (naive)
        }

        try {
            return ZoneId.of(timezoneName);
        } catch (Exception e) {
            // Invalid timezone in metadata - log and treat as naive
            return null;
        }
    }

    /**
     * Gets the time unit from the Arrow vector.
     *
     * @param vector The timestamp vector
     * @return The TimeUnit
     * @throws SQLException If the unit is not supported
     */
    protected static TimeUnit getTimeUnitForVector(TimeStampVector vector) throws SQLException {
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
