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

import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorGetter.Getter;
import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorGetter.Holder;
import static com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorGetter.createGetter;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromMilliseconds;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
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

    private final Getter getter;
    private final TimeUnit timeUnit;
    private final Holder holder;

    private static final String INVALID_VECTOR_ERROR_RESPONSE = "Unsupported Timestamp vector type provided";

    public TimeVectorAccessor(
            TimeNanoVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        super(currentRowSupplier, setCursorWasNull);
        this.holder = new TimeVectorGetter.Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public TimeVectorAccessor(
            TimeMicroVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        super(currentRowSupplier, setCursorWasNull);
        this.holder = new Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public TimeVectorAccessor(
            TimeMilliVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        super(currentRowSupplier, setCursorWasNull);
        this.holder = new Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public TimeVectorAccessor(
            TimeSecVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        super(currentRowSupplier, setCursorWasNull);
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
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
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

    @Override
    public String getString() {
        Time time = getTime(null);
        if (time == null) {
            return null;
        }

        return time.toLocalTime().format(DateTimeFormatter.ISO_TIME);
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
        throw new DataCloudJDBCException(INVALID_VECTOR_ERROR_RESPONSE, "22007", rootCauseException);
    }
}
