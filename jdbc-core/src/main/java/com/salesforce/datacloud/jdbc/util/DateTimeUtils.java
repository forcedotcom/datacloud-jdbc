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
package com.salesforce.datacloud.jdbc.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;
import lombok.val;

/** Datetime utility functions. */
public final class DateTimeUtils {

    public static final long MILLIS_TO_MICRO_SECS_CONVERSION_FACTOR = 1000;

    private DateTimeUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /** Subtracts default Calendar's timezone offset from epoch milliseconds to get relative UTC milliseconds */
    public static long applyCalendarOffset(long milliseconds) {
        final TimeZone defaultTz = TimeZone.getDefault();
        return milliseconds - defaultTz.getOffset(milliseconds);
    }

    public static long applyCalendarOffset(long milliseconds, Calendar calendar) {
        if (calendar == null) {
            return applyCalendarOffset(milliseconds);
        }
        val timeZone = calendar.getTimeZone();
        return milliseconds - timeZone.getOffset(milliseconds);
    }

    public static Date getUTCDateFromMilliseconds(long milliseconds) {
        return new Date(applyCalendarOffset(milliseconds));
    }

    public static Time getUTCTimeFromMilliseconds(long milliseconds) {
        return new Time(applyCalendarOffset(milliseconds));
    }

    public static Date getUTCDateFromDateAndCalendar(Date date, Calendar calendar) {
        val milliseconds = date.getTime();
        return new Date(applyCalendarOffset(milliseconds, calendar));
    }

    public static Time getUTCTimeFromTimeAndCalendar(Time time, Calendar calendar) {
        val milliseconds = time.getTime();
        return new Time(applyCalendarOffset(milliseconds, calendar));
    }

    public static Timestamp getUTCTimestampFromTimestampAndCalendar(Timestamp timestamp, Calendar calendar) {
        val milliseconds = timestamp.getTime();
        return new Timestamp(applyCalendarOffset(milliseconds, calendar));
    }

    /**
     * Converts LocalDateTime to microseconds since epoch.
     *
     * @param localDateTime The LocalDateTime to convert.
     * @return The microseconds since epoch.
     */
    public static long localDateTimeToMicrosecondsSinceEpoch(LocalDateTime localDateTime) {
        long epochMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        return millisToMicrosecondsSinceMidnight(epochMillis);
    }

    /**
     * Converts milliseconds since midnight to microseconds since midnight.
     *
     * @param millis The milliseconds since midnight.
     * @return The microseconds since midnight.
     */
    public static long millisToMicrosecondsSinceMidnight(long millis) {
        return millis * MILLIS_TO_MICRO_SECS_CONVERSION_FACTOR;
    }

    /**
     * Adjusts LocalDateTime for the given Calendar's timezone offset.
     *
     * @param localDateTime The LocalDateTime to adjust.
     * @param calendar The Calendar with the target timezone.
     * @param defaultTimeZone The default timezone to compare against.
     * @return The adjusted LocalDateTime.
     */
    public static LocalDateTime adjustForCalendar(
            LocalDateTime localDateTime, Calendar calendar, TimeZone defaultTimeZone) {
        if (calendar == null) {
            return localDateTime;
        }

        TimeZone targetTimeZone = calendar.getTimeZone();
        long millis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long offsetMillis = targetTimeZone.getOffset(millis) - (long) defaultTimeZone.getOffset(millis);
        return localDateTime.plus(offsetMillis, ChronoUnit.MILLIS);
    }
}
