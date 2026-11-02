/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.DataCloudInterval;
import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.util.function.IntSupplier;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;

/**
 * Presents a Hyper {@code INTERVAL} (Arrow {@code Interval(MONTH_DAY_NANO)}) as a
 * {@link DataCloudInterval}. {@link #getString()} returns the ISO-8601 text Hyper itself prints for
 * {@code interval::text}, e.g. {@code P1Y2M3DT4H5M6.789000S}.
 */
public class IntervalVectorAccessor extends QueryJDBCAccessor {

    private final IntervalMonthDayNanoVector vector;

    public IntervalVectorAccessor(IntervalMonthDayNanoVector vector, IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
        this.vector = vector;
    }

    @Override
    public Class<?> getObjectClass() {
        return DataCloudInterval.class;
    }

    @Override
    public DataCloudInterval getObject() {
        final int row = getCurrentRow();
        this.wasNull = vector.isNull(row);
        if (this.wasNull) {
            return null;
        }
        ArrowBuf buffer = vector.getDataBuffer();
        return new DataCloudInterval(
                IntervalMonthDayNanoVector.getMonths(buffer, row),
                IntervalMonthDayNanoVector.getDays(buffer, row),
                IntervalMonthDayNanoVector.getNanoseconds(buffer, row));
    }

    @Override
    public String getString() {
        DataCloudInterval interval = getObject();
        return interval == null ? null : interval.toString();
    }
}
