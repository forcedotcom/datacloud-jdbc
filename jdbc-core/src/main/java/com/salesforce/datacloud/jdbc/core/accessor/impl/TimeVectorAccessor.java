/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorGetter.Getter;
import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorGetter.Holder;
import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorGetter.createGetter;
import static com.salesforce.datacloud.jdbc.util.Constants.ISO_TIME_FORMAT_MICROS;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromMilliseconds;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

public class TimeVectorAccessor extends QueryJDBCAccessor {

    private final ValueVector vector;
    private final Getter getter;
    private final TimeUnit timeUnit;
    private final Holder holder;

    private static final String INVALID_VECTOR_ERROR_RESPONSE = "Unsupported Timestamp vector type provided";

    public TimeVectorAccessor(TimeNanoVector vector, IntSupplier currentRowSupplier) throws SQLException {
        super(currentRowSupplier);
        this.vector = vector;
        this.holder = new TimeVectorGetter.Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public TimeVectorAccessor(TimeMicroVector vector, IntSupplier currentRowSupplier) throws SQLException {
        super(currentRowSupplier);
        this.vector = vector;
        this.holder = new Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public TimeVectorAccessor(TimeMilliVector vector, IntSupplier currentRowSupplier) throws SQLException {
        super(currentRowSupplier);
        this.vector = vector;
        this.holder = new Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public TimeVectorAccessor(TimeSecVector vector, IntSupplier currentRowSupplier) throws SQLException {
        super(currentRowSupplier);
        this.vector = vector;
        this.holder = new Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    @Override
    public Class<?> getObjectClass() {
        return Time.class;
    }

    @Override
    public Object getObject() {
        return this.getTime(null);
    }

    /**
     * @param calendar Calendar passed in. Ignores the calendar
     * @return the Time relative to 00:00:00 assuming timezone is UTC
     */
    @Override
    public Time getTime(Calendar calendar) {
        fillHolder();
        if (this.wasNull) {
            return null;
        }

        long value = holder.value;
        long milliseconds = this.timeUnit.toMillis(value);

        return getUTCTimeFromMilliseconds(milliseconds);
    }

    private void fillHolder() {
        // Source wasNull from vector.isNull(int) rather than holder.isSet. Arrow's
        // Time*Vector.get(int, holder) currently honors validity unconditionally, but other
        // vector types (e.g. TimeStamp*) gate that path on arrow.enable_null_check_for_get;
        // sourcing from isNull keeps null detection independent of any future flag extension.
        final int row = getCurrentRow();
        this.wasNull = vector.isNull(row);
        if (this.wasNull) {
            return;
        }
        getter.get(row, holder);
    }

    /**
     * Lossless retrieval path: converts the raw Arrow value directly to a {@link LocalTime}
     * without routing through {@code java.sql.Time}, which only has millisecond resolution.
     */
    private LocalTime getLocalTime() {
        fillHolder();
        if (this.wasNull) {
            return null;
        }

        long nanoOfDay = this.timeUnit.toNanos(holder.value);
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    /**
     * @param calendar Calendar passed in. Ignores the calendar
     * @return the Timestamp relative to 00:00:00 assuming timezone is UTC
     */
    @Override
    public Timestamp getTimestamp(Calendar calendar) {
        Time time = getTime(calendar);
        if (time == null) {
            return null;
        }
        return new Timestamp(time.getTime());
    }

    /**
     * @return the TIME value formatted with microsecond precision, matching the scale advertised
     *     by result set metadata. Unlike {@link #getTime(Calendar)}, this does not route through
     *     {@code java.sql.Time} and therefore does not lose fractional precision below a
     *     millisecond.
     */
    @Override
    public String getString() {
        LocalTime localTime = getLocalTime();
        if (localTime == null) {
            return null;
        }

        return localTime.format(DateTimeFormatter.ofPattern(ISO_TIME_FORMAT_MICROS));
    }

    /**
     * @return the TIME value as a {@link LocalTime} when {@code type} is {@code LocalTime.class},
     *     the one lossless accessor for this column type. Falls back to the inherited behavior
     *     (raw {@link Time} object or {@link #getString()}) for every other requested type.
     */
    @Override
    public <T> T getObject(Class<T> type) throws SQLException {
        if (type == LocalTime.class) {
            return type.cast(getLocalTime());
        }
        return super.getObject(type);
    }

    protected static TimeUnit getTimeUnitForVector(ValueVector vector) throws SQLException {
        if (vector instanceof TimeNanoVector) {
            return TimeUnit.NANOSECONDS;
        } else if (vector instanceof TimeMicroVector) {
            return TimeUnit.MICROSECONDS;
        } else if (vector instanceof TimeMilliVector) {
            return TimeUnit.MILLISECONDS;
        } else if (vector instanceof TimeSecVector) {
            return TimeUnit.SECONDS;
        }

        val rootCauseException = new UnsupportedOperationException(INVALID_VECTOR_ERROR_RESPONSE);
        throw new SQLException(INVALID_VECTOR_ERROR_RESPONSE, "22007", rootCauseException);
    }
}
