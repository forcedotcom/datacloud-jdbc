/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import java.time.Duration;
import java.time.Period;

/**
 * A Hyper {@code INTERVAL} value, returned by {@code ResultSet.getObject} for {@code INTERVAL} columns.
 *
 * <p>Hyper stores an interval as three independent signed components: months, days and nanoseconds.
 * They cannot be folded into each other (a month is not a fixed number of days and a day is not
 * always 24 hours), so no single {@code java.time} class can represent every value. This class keeps
 * the components exactly; use {@link #toPeriod()} and {@link #toDuration()} for the {@code java.time}
 * views.
 *
 * <p>{@link #toString()} (and therefore {@code ResultSet.getString}) prints the ISO-8601 form Hyper
 * itself prints for {@code interval::text}, e.g. {@code P1Y2M3DT4H5M6.789000S}.
 */
public final class DataCloudInterval {

    private static final long NANOS_PER_MICRO = 1_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
    private static final long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

    private final int months;
    private final int days;
    private final long nanos;

    public DataCloudInterval(int months, int days, long nanos) {
        this.months = months;
        this.days = days;
        this.nanos = nanos;
    }

    /** The total number of months (years * 12 + months), negative for negative intervals. */
    public int getTotalMonths() {
        return months;
    }

    /** The number of days, which are never folded into months or hours. */
    public int getDays() {
        return days;
    }

    /** The time-of-day part in nanoseconds. Hyper itself is microsecond-granular. */
    public long getNanos() {
        return nanos;
    }

    /** The months and days as a {@link Period}; the time-of-day part is not included. */
    public Period toPeriod() {
        return Period.of(months / 12, months % 12, days);
    }

    /** The time-of-day part as a {@link Duration}; the months and days are not included. */
    public Duration toDuration() {
        return Duration.ofNanos(nanos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataCloudInterval)) {
            return false;
        }
        DataCloudInterval other = (DataCloudInterval) o;
        return months == other.months && days == other.days && nanos == other.nanos;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(months);
        result = 31 * result + Integer.hashCode(days);
        return 31 * result + Long.hashCode(nanos);
    }

    /**
     * Formats the interval the way Hyper prints it: components keep their own sign, years are
     * derived from the month count, days and hours never roll up into larger units, a zero interval
     * is {@code PT0S}, and fractional seconds are printed with six digits only when non-zero.
     */
    @Override
    public String toString() {
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
