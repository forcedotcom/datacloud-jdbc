/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DateUtility;

public class TimeStampVectorAccessor extends QueryJDBCAccessor {
    private static final String ISO_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String ISO_DATE_TIME_SEC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String INVALID_UNIT_ERROR_RESPONSE = "Invalid Arrow time unit";

    @FunctionalInterface
    interface LongToLocalDateTime {
        LocalDateTime fromLong(long value);
    }

    private final TimeZone timeZone;
    private final TimeUnit timeUnit;
    private final LongToLocalDateTime longToLocalDateTime;
    private final TimeStampVectorGetter.Holder holder;
    private final TimeStampVectorGetter.Getter getter;

    public TimeStampVectorAccessor(
            TimeStampVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer)
            throws SQLException {
        super(currentRowSupplier, wasNullConsumer);
        this.timeZone = getTimeZoneForVector(vector);
        this.timeUnit = getTimeUnitForVector(vector);
        this.longToLocalDateTime = getLongToLocalDateTimeForVector(vector, this.timeZone);
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

        if (this.timeUnit == TimeUnit.SECONDS) {
            return localDateTime.format(DateTimeFormatter.ofPattern(ISO_DATE_TIME_SEC_FORMAT));
        }

        return localDateTime.format(DateTimeFormatter.ofPattern(ISO_DATE_TIME_FORMAT));
    }

    private LocalDateTime getLocalDateTime(Calendar calendar) {
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
        if (this.wasNull) {
            return null;
        }

        long value = holder.value;
        LocalDateTime localDateTime = this.longToLocalDateTime.fromLong(value);

        if (calendar != null) {
            TimeZone timeZone = calendar.getTimeZone();
            ZonedDateTime utcZonedDateTime =
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.timeUnit.toMillis(value)), ZoneOffset.UTC);
            localDateTime =
                    utcZonedDateTime.withZoneSameInstant(timeZone.toZoneId()).toLocalDateTime();
        }
        return localDateTime;
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
}
