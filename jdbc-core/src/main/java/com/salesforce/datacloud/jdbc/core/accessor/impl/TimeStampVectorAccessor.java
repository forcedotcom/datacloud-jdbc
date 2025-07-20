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

import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorGetter.createGetter;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DateUtility;

/**
 * JDBC accessor for Arrow TimeStampVector types, handling both naive and timezone-aware timestamps.
 *
 * <p>This class provides JDBC-compatible access to timestamp data stored in Arrow vectors, with
 * comprehensive support for timezone handling and calendar-based conversions. It distinguishes
 * between two types of timestamps:
 *
 * <h3>Timestamp Types</h3>
 * <ul>
 *   <li><strong>Naive timestamps</strong> (timestamp): No timezone information in the vector metadata</li>
 *   <li><strong>Timezone-aware timestamps</strong> (timestamptz): Contains timezone information in the vector metadata</li>
 * </ul>
 *
 * <h3>Key Features</h3>
 * <ul>
 *   <li><strong>Avatica Framework Integration</strong>: Detects and handles calendars automatically injected by Avatica</li>
 *   <li><strong>Session Timezone Support</strong>: Uses connection properties to determine session timezone context</li>
 *   <li><strong>Flexible Calendar Handling</strong>: Supports user-provided calendars for timezone conversions</li>
 *   <li><strong>Multi-precision Support</strong>: Handles nanosecond, microsecond, millisecond, and second precision</li>
 * </ul>
 *
 * <h3>Timezone Handling Strategy</h3>
 * <p>The class implements sophisticated timezone handling logic:
 * <ul>
 *   <li>For naive timestamps: Uses Avatica detection to determine whether to apply calendar conversions</li>
 *   <li>For timezone-aware timestamps: Always respects vector timezone information and calendar conversions</li>
 *   <li>UTC is used as the reference timezone for all naive timestamp interpretations</li>
 * </ul>
 *
 * <h3>Usage Example</h3>
 * <pre>
 * // Create accessor with connection properties for session timezone
 * TimeStampVectorAccessor accessor = new TimeStampVectorAccessor(
 *     timeStampVector,
 *     rowSupplier,
 *     wasNullConsumer,
 *     connectionProperties
 * );
 *
 * // Get timestamp with automatic timezone handling
 * Timestamp timestamp = accessor.getTimestamp(null);
 *
 * // Get timestamp with user-specified timezone
 * Calendar userCalendar = Calendar.getInstance(TimeZone.getTimeZone("Europe/London"));
 * Timestamp converted = accessor.getTimestamp(userCalendar);
 * </pre>
 *
 * @see QueryJDBCAccessor
 * @see TimeStampVector
 * @see ConnectionQuerySettings
 */
public class TimeStampVectorAccessor extends QueryJDBCAccessor {
    private static final String ISO_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String ISO_DATE_TIME_SEC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String ISO_DATE_TIME_NAIVE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String ISO_DATE_TIME_NAIVE_SEC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    private static final String INVALID_UNIT_ERROR_RESPONSE = "Invalid Arrow time unit";

    @FunctionalInterface
    interface LongToLocalDateTime {
        LocalDateTime fromLong(long value);
    }

    private final TimeZone timeZone;
    private final TimeZone sessionTimeZone;
    private final TimeUnit timeUnit;
    private final LongToLocalDateTime longToLocalDateTime;
    private final TimeStampVectorGetter.Holder holder;
    private final TimeStampVectorGetter.Getter getter;
    private final boolean hasTimezoneInfo;

    public TimeStampVectorAccessor(
            TimeStampVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer)
            throws SQLException {
        this(vector, currentRowSupplier, wasNullConsumer, null);
    }

    public TimeStampVectorAccessor(
            TimeStampVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer,
            Properties connectionProperties)
            throws SQLException {
        super(currentRowSupplier, wasNullConsumer);
        this.timeZone = getTimeZoneForVector(vector);
        this.sessionTimeZone = getSessionTimeZone(connectionProperties);
        this.hasTimezoneInfo = hasTimezoneInfo(vector);
        this.timeUnit = getTimeUnitForVector(vector);

        // For naive timestamps, use UTC for initial conversion to avoid timezone offset
        // For timezone-aware timestamps, use the vector's timezone
        TimeZone conversionTimeZone = hasTimezoneInfo ? timeZone : TimeZone.getTimeZone("UTC");
        this.longToLocalDateTime = getLongToLocalDateTimeForVector(vector, conversionTimeZone);

        this.holder = new TimeStampVectorGetter.Holder();
        this.getter = createGetter(vector);
    }

    @Override
    public Date getDate(Calendar calendar) {
        LocalDateTime localDateTime = getLocalDateTime(calendar);
        if (localDateTime == null) {
            return null;
        }

        return new Date(Timestamp.valueOf(localDateTime).getTime());
    }

    @Override
    public Time getTime(Calendar calendar) {
        LocalDateTime localDateTime = getLocalDateTime(calendar);
        if (localDateTime == null) {
            return null;
        }

        return new Time(Timestamp.valueOf(localDateTime).getTime());
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) {
        LocalDateTime localDateTime = getLocalDateTime(calendar);
        if (localDateTime == null) {
            return null;
        }

        return Timestamp.valueOf(localDateTime);
    }

    @Override
    public Class<?> getObjectClass() {
        return Timestamp.class;
    }

    @Override
    public Object getObject() {
        return this.getTimestamp(null);
    }

    @Override
    public String getString() {
        LocalDateTime localDateTime = getLocalDateTime(null);
        if (localDateTime == null) {
            return null;
        }

        // Use appropriate format based on whether timestamp has timezone info
        if (hasTimezoneInfo) {
            // For timezone-aware timestamps, include 'Z' suffix
            if (this.timeUnit == TimeUnit.SECONDS) {
                return localDateTime.format(DateTimeFormatter.ofPattern(ISO_DATE_TIME_SEC_FORMAT));
            }
            return localDateTime.format(DateTimeFormatter.ofPattern(ISO_DATE_TIME_FORMAT));
        } else {
            // For naive timestamps, don't include timezone indicator
            if (this.timeUnit == TimeUnit.SECONDS) {
                return localDateTime.format(DateTimeFormatter.ofPattern(ISO_DATE_TIME_NAIVE_SEC_FORMAT));
            }
            return localDateTime.format(DateTimeFormatter.ofPattern(ISO_DATE_TIME_NAIVE_FORMAT));
        }
    }

    /**
     * Converts the raw timestamp value to a LocalDateTime, handling both naive and timezone-aware timestamps.
     *
     * <p>This method implements different conversion strategies based on whether the timestamp has timezone
     * information and the presence/nature of the provided Calendar parameter.
     *
     * <h3>Naive Timestamps (no timezone info)</h3>
     * For naive timestamps (timestamp without timezone), the method uses a three-case logic:
     *
     * <h4>Case 1: Avatica Detection (Calendar matches session timezone)</h4>
     * When a Calendar is provided whose timezone matches the session timezone, it's assumed to be
     * automatically injected by the Avatica framework rather than explicitly provided by the user.
     * In this case, the Calendar is ignored and literal timestamp values are returned.
     *
     * <pre>
     * Example:
     *   Session timezone: America/Los_Angeles
     *   Calendar timezone: America/Los_Angeles
     *   Raw value: 1704110400000 (2024-01-01T12:00:00Z)
     *   Result: 2024-01-01T12:00:00 (literal UTC interpretation)
     * </pre>
     *
     * <h4>Case 2: User-provided Calendar (Different timezone)</h4>
     * When a Calendar is provided with a timezone different from the session timezone, it's assumed
     * to be explicitly provided by the user. The timestamp is converted from UTC to the Calendar's timezone.
     *
     * <pre>
     * Example:
     *   Session timezone: America/Los_Angeles
     *   Calendar timezone: Europe/London
     *   Raw value: 1704110400000 (2024-01-01T12:00:00Z)
     *   Result: 2024-01-01T13:00:00 (converted to London time)
     * </pre>
     *
     * <h4>Case 3: No Calendar</h4>
     * When no Calendar is provided, literal timestamp values are returned with UTC interpretation.
     *
     * <pre>
     * Example:
     *   Raw value: 1704110400000 (2024-01-01T12:00:00Z)
     *   Result: 2024-01-01T12:00:00 (literal UTC interpretation)
     * </pre>
     *
     * <h3>Timezone-aware Timestamps (with timezone info)</h3>
     * For timezone-aware timestamps (timestamptz), the vector contains timezone information which is
     * used as the source timezone for conversions. If a Calendar is provided, the timestamp is
     * converted from the vector's timezone to the Calendar's timezone.
     *
     * <h3>Edge Cases and Considerations</h3>
     * <ul>
     *   <li><strong>Ambiguous Avatica Detection:</strong> If a user explicitly provides a Calendar
     *       that happens to match the session timezone, it will be treated as an Avatica-injected
     *       Calendar and ignored. This is a rare edge case.</li>
     *   <li><strong>Null Handling:</strong> Returns null if the timestamp value is null.</li>
     *   <li><strong>UTC Reference:</strong> All naive timestamps are interpreted with UTC as the
     *       reference timezone before any calendar conversions.</li>
     * </ul>
     *
     * @param calendar Optional Calendar for timezone conversion. May be null.
     * @return LocalDateTime representation of the timestamp, or null if the value is null
     * @see #getTimestamp(Calendar)
     * @see #getSessionTimeZone(Properties)
     */
    private LocalDateTime getLocalDateTime(Calendar calendar) {
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
        if (this.wasNull) {
            return null;
        }

        long value = holder.value;

        /*
         * NAIVE TIMESTAMP HANDLING (no timezone information)
         *
         * For naive timestamps, we implement a three-case logic to handle Calendar parameters:
         * 1. Calendar matches session timezone -> Likely from Avatica, ignore it
         * 2. Calendar differs from session timezone -> User-provided, respect it
         * 3. No calendar -> Return literal values
         */
        if (!hasTimezoneInfo) {
            if (calendar != null && calendar.getTimeZone().equals(sessionTimeZone)) {
                // Case 1: Avatica Detection - Calendar matches session timezone
                // This suggests the Calendar was automatically injected by Avatica framework
                // rather than explicitly provided by the user. For naive timestamps, we want
                // to preserve literal values, so we ignore the Calendar.
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(this.timeUnit.toMillis(value)), ZoneOffset.UTC);
            } else if (calendar != null) {
                // Case 2: User-provided Calendar - Different timezone from session
                // This suggests the user explicitly provided a Calendar for timezone conversion.
                // We respect the user's intent and convert from UTC to the Calendar's timezone.
                TimeZone calendarTimeZone = calendar.getTimeZone();
                ZonedDateTime utcZonedDateTime =
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.timeUnit.toMillis(value)), ZoneOffset.UTC);
                return utcZonedDateTime
                        .withZoneSameInstant(calendarTimeZone.toZoneId())
                        .toLocalDateTime();
            } else {
                // Case 3: No Calendar - Return literal values
                // Without any Calendar guidance, we return the literal timestamp values
                // interpreted as UTC (which is appropriate for naive timestamps).
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(this.timeUnit.toMillis(value)), ZoneOffset.UTC);
            }
        }

        /*
         * TIMEZONE-AWARE TIMESTAMP HANDLING (with timezone information)
         *
         * For timezone-aware timestamps (timestamptz), the Arrow vector contains timezone information
         * that specifies the source timezone. We use this information for proper timezone conversions.
         */
        if (calendar != null) {
            // Calendar provided: Convert from vector's timezone to calendar's timezone
            // This is a standard timezone conversion where we:
            // 1. Interpret the raw value using the vector's timezone (source)
            // 2. Convert to the calendar's timezone (target)
            // 3. Return the LocalDateTime representation
            TimeZone calendarTimeZone = calendar.getTimeZone();
            ZonedDateTime vectorZonedDateTime =
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.timeUnit.toMillis(value)), timeZone.toZoneId());
            return vectorZonedDateTime
                    .withZoneSameInstant(calendarTimeZone.toZoneId())
                    .toLocalDateTime();
        } else {
            // No calendar provided: Use the longToLocalDateTime converter
            // This converter uses the vector's timezone information and session timezone
            // to produce the appropriate LocalDateTime representation. The exact behavior
            // depends on the timezone conversion logic in DateUtility methods.
            return this.longToLocalDateTime.fromLong(value);
        }
    }

    private static LongToLocalDateTime getLongToLocalDateTimeForVector(TimeStampVector vector, TimeZone timeZone)
            throws SQLException {
        String timeZoneID = timeZone.getID();

        ArrowType.Timestamp arrowType =
                (ArrowType.Timestamp) vector.getField().getFieldType().getType();

        switch (arrowType.getUnit()) {
            case NANOSECOND:
                return nanoseconds -> DateUtility.getLocalDateTimeFromEpochNano(nanoseconds, timeZoneID);
            case MICROSECOND:
                return microseconds -> DateUtility.getLocalDateTimeFromEpochMicro(microseconds, timeZoneID);
            case MILLISECOND:
                return milliseconds -> DateUtility.getLocalDateTimeFromEpochMilli(milliseconds, timeZoneID);
            case SECOND:
                return seconds ->
                        DateUtility.getLocalDateTimeFromEpochMilli(TimeUnit.SECONDS.toMillis(seconds), timeZoneID);
            default:
                val rootCauseException = new UnsupportedOperationException(INVALID_UNIT_ERROR_RESPONSE);
                throw new DataCloudJDBCException(INVALID_UNIT_ERROR_RESPONSE, "22007", rootCauseException);
        }
    }

    /**
     * Extracts the timezone information from a TimeStampVector's Arrow metadata.
     *
     * <p>For timezone-aware timestamp vectors (timestamptz), the Arrow schema contains timezone
     * information that specifies how the timestamp values should be interpreted. This method
     * extracts that timezone information and returns an appropriate TimeZone object.
     *
     * <p>If no timezone information is present in the vector (naive timestamps), UTC is returned
     * as the default reference timezone.
     *
     * @param vector the TimeStampVector containing timezone metadata
     * @return the TimeZone specified in the vector's metadata, or UTC if no timezone is specified
     */
    protected static TimeZone getTimeZoneForVector(TimeStampVector vector) {
        ArrowType.Timestamp arrowType =
                (ArrowType.Timestamp) vector.getField().getFieldType().getType();

        String timezoneName = arrowType.getTimezone();

        return timezoneName == null ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezoneName);
    }

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
                throw new DataCloudJDBCException(INVALID_UNIT_ERROR_RESPONSE, "22007", rootCauseException);
        }
    }

    /**
     * Determines whether a TimeStampVector contains timezone information.
     *
     * <p>This method checks the Arrow metadata to determine if the timestamp vector represents:
     * <ul>
     *   <li><strong>Naive timestamps</strong> (timestamp) - no timezone information</li>
     *   <li><strong>Timezone-aware timestamps</strong> (timestamptz) - contains timezone information</li>
     * </ul>
     *
     * <p>The presence of timezone information affects how timestamps are interpreted and converted.
     * Naive timestamps are treated as literal values, while timezone-aware timestamps undergo
     * timezone conversions based on the vector's timezone metadata.
     *
     * @param vector the TimeStampVector to check for timezone information
     * @return true if the vector contains timezone information (timestamptz), false otherwise (timestamp)
     */
    static boolean hasTimezoneInfo(TimeStampVector vector) {
        ArrowType.Timestamp arrowType =
                (ArrowType.Timestamp) vector.getField().getFieldType().getType();
        return arrowType.getTimezone() != null;
    }

    /**
     * Determines the session timezone from connection properties.
     *
     * <p>The session timezone is used for interpreting naive timestamps and for the Avatica
     * detection logic. It represents the timezone context in which the database session
     * is operating.
     *
     * <p>If connection properties are provided, the session timezone is extracted from the
     * {@code querySetting.timezone} property. If no properties are provided or no timezone
     * is specified, the JVM's default timezone is used.
     *
     * @param connectionProperties connection properties containing timezone settings, may be null
     * @return the session timezone, or JVM default timezone if not specified
     * @see ConnectionQuerySettings#getSessionTimeZone()
     */
    static TimeZone getSessionTimeZone(Properties connectionProperties) {
        if (connectionProperties == null) {
            return TimeZone.getDefault();
        }

        String timezoneProp = connectionProperties.getProperty("querySetting.timezone");
        if (timezoneProp == null || timezoneProp.trim().isEmpty()) {
            return TimeZone.getDefault();
        }

        try {
            TimeZone timeZone = TimeZone.getTimeZone(timezoneProp);
            // TimeZone.getTimeZone() returns GMT for invalid timezone strings
            // Check if we got GMT but didn't ask for it
            if ("GMT".equals(timeZone.getID()) && !timezoneProp.equals("GMT") && !timezoneProp.equals("UTC")) {
                return TimeZone.getDefault();
            }
            return timeZone;
        } catch (Exception e) {
            // If timezone parsing fails, fall back to default
            return TimeZone.getDefault();
        }
    }

    private TimeZone getEffectiveTimeZone() {
        // if schema has timezone info, use it
        if (hasTimezoneInfo) {
            return timeZone;
        }
        // otherwise, use session timezone for timestamp interpretation
        return sessionTimeZone;
    }
}
