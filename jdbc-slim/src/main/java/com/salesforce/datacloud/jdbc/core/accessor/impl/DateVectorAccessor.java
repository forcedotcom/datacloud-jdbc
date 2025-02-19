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

import static com.salesforce.datacloud.jdbc.core.accessor.impl.DateVectorGetter.createGetter;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCDateFromMilliseconds;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorFactory;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DateVectorGetter.Getter;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DateVectorGetter.Holder;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import lombok.val;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.ValueVector;

public class DateVectorAccessor extends QueryJDBCAccessor {

    private final Getter getter;
    private final TimeUnit timeUnit;
    private final Holder holder;

    private static final String INVALID_VECTOR_ERROR_RESPONSE = "Invalid Arrow vector provided";

    public DateVectorAccessor(
            DateDayVector vector,
            IntSupplier currentRowSupplier,
            QueryJDBCAccessorFactory.WasNullConsumer setCursorWasNull)
            throws SQLException {
        super(currentRowSupplier, setCursorWasNull);
        this.holder = new Holder();
        this.getter = createGetter(vector);
        this.timeUnit = getTimeUnitForVector(vector);
    }

    public DateVectorAccessor(
            DateMilliVector vector,
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
        return Date.class;
    }

    @Override
    public Object getObject() {
        return this.getDate(null);
    }

    /**
     * @param calendar Calendar passed in. Ignores the calendar
     * @return Timestamp of Date at current row, at midnight UTC
     */
    @Override
    public Timestamp getTimestamp(Calendar calendar) {
        Date date = getDate(calendar);
        if (date == null) {
            return null;
        }
        return new Timestamp(date.getTime());
    }

    /**
     * @param calendar Calendar passed in. Ignores the calendar
     * @return Date of current row in UTC
     */
    @Override
    public Date getDate(Calendar calendar) {
        fillHolder();
        if (this.wasNull) {
            return null;
        }

        long value = holder.value;
        long milliseconds = this.timeUnit.toMillis(value);

        return getUTCDateFromMilliseconds(milliseconds);
    }

    @Override
    public String getString() {
        val date = getDate(null);
        if (date == null) {
            return null;
        }

        return date.toLocalDate().toString();
    }

    private void fillHolder() {
        getter.get(getCurrentRow(), holder);
        this.wasNull = holder.isSet == 0;
        this.wasNullConsumer.setWasNull(this.wasNull);
    }

    protected static TimeUnit getTimeUnitForVector(ValueVector vector) throws SQLException {
        if (vector instanceof DateDayVector) {
            return TimeUnit.DAYS;
        } else if (vector instanceof DateMilliVector) {
            return TimeUnit.MILLISECONDS;
        }

        val rootCauseException = new IllegalArgumentException(INVALID_VECTOR_ERROR_RESPONSE);
        throw new DataCloudJDBCException(INVALID_VECTOR_ERROR_RESPONSE, "22007", rootCauseException);
    }
}
