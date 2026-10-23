/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor;
import java.util.function.IntSupplier;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;

/**
 * Presents a Hyper {@code INTERVAL} (Arrow {@code Interval(MONTH_DAY_NANO)}) as the ISO-8601
 * duration string Hyper itself prints for {@code interval::text}, e.g. {@code P1Y2M3DT4H5M6.789000S}.
 *
 * <p>JDBC has no interval type and no single {@code java.time} class can hold all three
 * independent components, so the value is surfaced as a {@link String}.
 */
public class IntervalVectorAccessor extends QueryJDBCAccessor {

    private static final long NANOS_PER_MICRO = 1_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
    private static final long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

    private final IntervalMonthDayNanoVector vector;

    public IntervalVectorAccessor(IntervalMonthDayNanoVector vector, IntSupplier currentRowSupplier) {
        super(currentRowSupplier);
        this.vector = vector;
    }

    @Override
    public Class<?> getObjectClass() {
        return String.class;
    }

    @Override
    public String getString() {
        final int row = getCurrentRow();
        this.wasNull = vector.isNull(row);
        if (this.wasNull) {
            return null;
        }
        ArrowBuf buffer = vector.getDataBuffer();
        return format(
                IntervalMonthDayNanoVector.getMonths(buffer, row),
                IntervalMonthDayNanoVector.getDays(buffer, row),
                IntervalMonthDayNanoVector.getNanoseconds(buffer, row));
    }

    @Override
    public String getObject() {
        return getString();
    }

    /**
     * Formats an interval the way Hyper prints it: components keep their own sign, years are
     * derived from the month count, days and hours never roll up into larger units, a zero interval
     * is {@code PT0S}, and fractional seconds are printed with six digits only when non-zero.
     */
    static String format(int months, int days, long nanos) {
        if (months == 0 && days == 0 && nanos == 0) {
            return "PT0S";
        }
        StringBuilder sb = new StringBuilder("P");
        int years = months / 12;
        int remainingMonths = months % 12;
        if (years != 0) {
            sb.append(years).append('Y');
        }
        if (remainingMonths != 0) {
            sb.append(remainingMonths).append('M');
        }
        if (days != 0) {
            sb.append(days).append('D');
        }
        if (nanos != 0) {
            sb.append('T');
            appendTime(sb, nanos);
        }
        return sb.toString();
    }

    private static void appendTime(StringBuilder sb, long nanos) {
        // Math.abs(Long.MIN_VALUE) is negative, so peel the sign off by hand via the remainders.
        long hours = nanos / NANOS_PER_HOUR;
        long rest = nanos % NANOS_PER_HOUR;
        long minutes = rest / NANOS_PER_MINUTE;
        rest = rest % NANOS_PER_MINUTE;
        long seconds = rest / NANOS_PER_SECOND;
        long fraction = rest % NANOS_PER_SECOND;

        if (hours != 0) {
            sb.append(hours).append('H');
        }
        if (minutes != 0) {
            sb.append(minutes).append('M');
        }
        if (seconds != 0 || fraction != 0) {
            // The time part is a single signed value, so every component shares its sign.
            if (seconds == 0 && fraction < 0) {
                sb.append('-');
            }
            sb.append(seconds);
            if (fraction != 0) {
                long absFraction = fraction < 0 ? -fraction : fraction;
                // Hyper is microsecond-granular; fall back to nanoseconds only if a value has them.
                if (absFraction % NANOS_PER_MICRO == 0) {
                    sb.append('.').append(String.format("%06d", absFraction / NANOS_PER_MICRO));
                } else {
                    sb.append('.').append(String.format("%09d", absFraction));
                }
            }
            sb.append('S');
        }
    }
}
