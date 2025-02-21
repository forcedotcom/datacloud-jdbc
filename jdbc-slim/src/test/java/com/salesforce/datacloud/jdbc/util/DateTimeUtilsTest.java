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

import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCDateFromDateAndCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCDateFromMilliseconds;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromMilliseconds;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimeFromTimeAndCalendar;
import static com.salesforce.datacloud.jdbc.util.DateTimeUtils.getUTCTimestampFromTimestampAndCalendar;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import lombok.val;
import org.junit.jupiter.api.Test;

public class DateTimeUtilsTest {

    private final long positiveEpochMilli = 959817600000L; // 2000-06-01 00:00:00 UTC
    private final long negativeEpochMilli = -618105600000L; // 1950-06-01 00:00:00 UTC

    private final long zeroEpochMilli = 0L; // 1970-01-01 00:00:00 UTC

    private final long maxMillisecondsInDay = 86400000L - 1; // 23:59:59

    private final long randomMillisecondsInDay = 44444444L; // 12:20:44

    private final ZoneId UTCZoneId = TimeZone.getTimeZone("UTC").toZoneId();

    @Test
    void testShouldGetUTCDateFromEpochZero() {
        // actual Date 1970, 01, 01 UTC
        val instant = Instant.ofEpochMilli(zeroEpochMilli);
        val zdt = ZonedDateTime.ofInstant(instant, UTCZoneId);

        val expectedYear = zdt.getYear();
        val expectedMonth = zdt.getMonthValue();
        val expectedDay = zdt.getDayOfMonth();

        val actual = getUTCDateFromMilliseconds(zeroEpochMilli);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);
    }

    @Test
    void testShouldGetUTCDateFromPositiveEpoch() {
        // actual Date 2000-06-01 00:00:00 UTC
        val instant = Instant.ofEpochMilli(positiveEpochMilli);
        val zdt = ZonedDateTime.ofInstant(instant, UTCZoneId);

        val expectedYear = zdt.getYear();
        val expectedMonth = zdt.getMonthValue();
        val expectedDay = zdt.getDayOfMonth();

        val actual = getUTCDateFromMilliseconds(positiveEpochMilli);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);
    }

    @Test
    void testShouldGetUTCDateFromNegativeEpoch() {
        // actual Date 1950-06-01 00:00:00 UTC
        val instant = Instant.ofEpochMilli(negativeEpochMilli);
        val zdt = ZonedDateTime.ofInstant(instant, UTCZoneId);

        val expectedYear = zdt.getYear();
        val expectedMonth = zdt.getMonthValue();
        val expectedDay = zdt.getDayOfMonth();

        val actual = getUTCDateFromMilliseconds(negativeEpochMilli);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);
    }

    @Test
    void testShouldGetUTCTimeFromEpochZero() {
        // actual Time 00:00:00.000 UTC
        val instant = Instant.ofEpochMilli(zeroEpochMilli);
        val zdt = ZonedDateTime.ofInstant(instant, UTCZoneId);

        val expectedHour = zdt.getHour();
        val expectedMinute = zdt.getMinute();
        val expectedSeconds = zdt.getSecond();
        val expectedMillis = (int) TimeUnit.NANOSECONDS.toMillis(zdt.getNano());

        val actual = getUTCTimeFromMilliseconds(zeroEpochMilli);

        assertThat(actual)
                .hasHourOfDay(expectedHour)
                .hasMinute(expectedMinute)
                .hasSecond(expectedSeconds)
                .hasMillisecond(expectedMillis);
    }

    @Test
    void testShouldGetUTCTimeFromMaxMillisecondsInDay() {
        // actual Time 23:59:59.999 UTC
        val instant = Instant.ofEpochMilli(maxMillisecondsInDay);
        val zdt = ZonedDateTime.ofInstant(instant, UTCZoneId);

        val expectedHour = zdt.getHour();
        val expectedMinute = zdt.getMinute();
        val expectedSeconds = zdt.getSecond();
        val expectedMillis = (int) TimeUnit.NANOSECONDS.toMillis(zdt.getNano());

        val actual = getUTCTimeFromMilliseconds(maxMillisecondsInDay);

        assertThat(actual)
                .hasHourOfDay(expectedHour)
                .hasMinute(expectedMinute)
                .hasSecond(expectedSeconds)
                .hasMillisecond(expectedMillis);
    }

    @Test
    void testShouldGetUTCTimeFromRandomMillisecondsInDay() {
        // actual Time 12:20:44 UTC
        val instant = Instant.ofEpochMilli(randomMillisecondsInDay);
        val zdt = ZonedDateTime.ofInstant(instant, UTCZoneId);

        val expectedHour = zdt.getHour();
        val expectedMinute = zdt.getMinute();
        val expectedSeconds = zdt.getSecond();
        val expectedMillis = (int) TimeUnit.NANOSECONDS.toMillis(zdt.getNano());

        val actual = getUTCTimeFromMilliseconds(randomMillisecondsInDay);

        assertThat(actual)
                .hasHourOfDay(expectedHour)
                .hasMinute(expectedMinute)
                .hasSecond(expectedSeconds)
                .hasMillisecond(expectedMillis);
    }

    @Test
    void testShouldGetUTCDateFromEpochZeroDifferentCalendar() {
        // UTC Date would be 1969-12-31
        TimeZone timeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(timeZone);

        val date = Date.valueOf("1970-01-01");
        val time = date.getTime();

        val dateTime = new Timestamp(time).toLocalDateTime();

        val zonedDateTime = dateTime.atZone(timeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();

        val actual = getUTCDateFromDateAndCalendar(date, calendar);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);
    }

    @Test
    void testShouldGetUTCDateFromEpochZeroSameCalendar() {
        // UTC Date would be 1970-01-01
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);

        val date = Date.valueOf("1970-01-01");
        val time = date.getTime();

        val dateTime = new Timestamp(time).toLocalDateTime();

        val zonedDateTime = dateTime.atZone(timeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();

        val actual = getUTCDateFromDateAndCalendar(date, calendar);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);
    }

    @Test
    void testShouldGetUTCDateFromEpochZeroNullCalendar() {
        // UTC Date would be 1970-01-01
        TimeZone defaultTz = TimeZone.getDefault();
        TimeZone timeZone = TimeZone.getTimeZone("GMT-8");
        TimeZone.setDefault(timeZone);

        val date = Date.valueOf("1970-01-01");
        val time = date.getTime();

        val dateTime = new Timestamp(time).toLocalDateTime();

        val zonedDateTime = dateTime.atZone(timeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();

        val actual = getUTCDateFromDateAndCalendar(date, null);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);

        TimeZone.setDefault(defaultTz);
    }

    @Test
    void testShouldGetUTCDateFromNegativeEpochGivenCalendar() {
        // UTC Date would be 1949-12-31 22:00
        TimeZone timeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(timeZone);

        val date = Date.valueOf("1950-01-01");
        val time = date.getTime();

        val dateTime = new Timestamp(time).toLocalDateTime();

        val zonedDateTime = dateTime.atZone(timeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();

        val actual = getUTCDateFromDateAndCalendar(date, calendar);

        assertThat(actual).hasYear(expectedYear).hasMonth(expectedMonth).hasDayOfMonth(expectedDay);
    }

    @Test
    void testShouldGetUTCTimeFromEpochZeroDifferentCalendar() {
        // UTC Time would be 22:00:00
        TimeZone timeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(timeZone);

        val time = Time.valueOf("00:00:00");

        val actual = getUTCTimeFromTimeAndCalendar(time, calendar);

        assertThat(actual).hasHourOfDay(22).hasMinute(0).hasSecond(0).hasMillisecond(0);
    }

    @Test
    void testShouldGetUTCTimeFromEpochZeroSameCalendar() {
        // UTC Time would be 00:00:00
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);

        val time = Time.valueOf("00:00:00");

        val actual = getUTCTimeFromTimeAndCalendar(time, calendar);

        assertThat(actual).hasHourOfDay(0).hasMinute(0).hasSecond(0).hasMillisecond(0);
    }

    @Test
    void testShouldGetUTCTimeFromEpochZeroNullCalendar() {
        // UTC Time would be 08:00:00
        TimeZone defaultTz = TimeZone.getDefault();
        TimeZone timeZone = TimeZone.getTimeZone("GMT-8");
        TimeZone.setDefault(timeZone);

        val time = Time.valueOf("00:00:00");

        val actual = getUTCTimeFromTimeAndCalendar(time, null);

        assertThat(actual).hasHourOfDay(8).hasMinute(0).hasSecond(0).hasMillisecond(0);

        TimeZone.setDefault(defaultTz);
    }

    @Test
    void testShouldGetUTCTimestampFromEpochZeroDifferentCalendar() {
        // UTC Timestamp would be 1969-12-31 22:00:00.000000000
        TimeZone plusTwoTimeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(plusTwoTimeZone);

        val timestamp = Timestamp.valueOf("1970-01-01 00:00:00.000000000");

        val dateTime = timestamp.toLocalDateTime();

        val zonedDateTime = dateTime.atZone(plusTwoTimeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();
        val expectedHour = finalDateTime.getHour();
        val expectedMinute = finalDateTime.getMinute();
        val expectedSecond = finalDateTime.getSecond();
        val expectedMillis = (int) TimeUnit.NANOSECONDS.toMillis(finalDateTime.getNano());

        val actual = getUTCTimestampFromTimestampAndCalendar(timestamp, calendar);

        assertThat(actual)
                .hasYear(expectedYear)
                .hasMonth(expectedMonth)
                .hasDayOfMonth(expectedDay)
                .hasHourOfDay(expectedHour)
                .hasMinute(expectedMinute)
                .hasSecond(expectedSecond)
                .hasMillisecond(expectedMillis);
    }

    @Test
    void testShouldGetUTCTimestampFromEpochZeroSameCalendar() {
        // UTC Timestamp would be 1970-01-01 00:00:00.000000000
        TimeZone UTCTimeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(UTCTimeZone);

        val timestamp = Timestamp.valueOf("1970-01-01 00:00:00.000000000");

        val dateTime = timestamp.toLocalDateTime();

        val zonedDateTime = dateTime.atZone(UTCTimeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();
        val expectedHour = finalDateTime.getHour();
        val expectedMinute = finalDateTime.getMinute();
        val expectedSecond = finalDateTime.getSecond();
        val expectedMillis = (int) TimeUnit.NANOSECONDS.toMillis(finalDateTime.getNano());

        val actual = getUTCTimestampFromTimestampAndCalendar(timestamp, calendar);

        assertThat(actual)
                .hasYear(expectedYear)
                .hasMonth(expectedMonth)
                .hasDayOfMonth(expectedDay)
                .hasHourOfDay(expectedHour)
                .hasMinute(expectedMinute)
                .hasSecond(expectedSecond)
                .hasMillisecond(expectedMillis);
    }

    @Test
    void testShouldGetUTCTimestampFromEpochZeroNullCalendar() {
        // UTC Time would be 1970-01-01 08:00:00.000000000
        TimeZone defaultTz = TimeZone.getDefault();
        TimeZone minusEightTimeZone = TimeZone.getTimeZone("GMT-8");
        TimeZone.setDefault(minusEightTimeZone);

        val timestamp = Timestamp.valueOf("1970-01-01 00:00:00.000000000");

        val dateTime = timestamp.toLocalDateTime();

        val zonedDateTime = dateTime.atZone(minusEightTimeZone.toZoneId());
        val convertedDateTime = zonedDateTime.withZoneSameInstant(UTCZoneId);
        val finalDateTime = convertedDateTime.toLocalDateTime();

        val expectedYear = finalDateTime.getYear();
        val expectedMonth = finalDateTime.getMonthValue();
        val expectedDay = finalDateTime.getDayOfMonth();
        val expectedHour = finalDateTime.getHour();
        val expectedMinute = finalDateTime.getMinute();
        val expectedSecond = finalDateTime.getSecond();
        val expectedMillis = (int) TimeUnit.NANOSECONDS.toMillis(finalDateTime.getNano());

        val actual = getUTCTimestampFromTimestampAndCalendar(timestamp, null);

        assertThat(actual)
                .hasYear(expectedYear)
                .hasMonth(expectedMonth)
                .hasDayOfMonth(expectedDay)
                .hasHourOfDay(expectedHour)
                .hasMinute(expectedMinute)
                .hasSecond(expectedSecond)
                .hasMillisecond(expectedMillis);

        TimeZone.setDefault(defaultTz);
    }

    @Test
    void testMillisToMicrosecondsSinceMidnight() {
        long millisSinceMidnight = 3600000; // 1 hour in milliseconds
        long expectedMicroseconds = millisSinceMidnight * 1000;
        assertEquals(expectedMicroseconds, DateTimeUtils.millisToMicrosecondsSinceMidnight(millisSinceMidnight));
    }

    @Test
    void testLocalDateTimeToMicrosecondsSinceEpoch() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        long expectedMicroseconds = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000;
        assertEquals(expectedMicroseconds, DateTimeUtils.localDateTimeToMicrosecondsSinceEpoch(localDateTime));
    }

    @Test
    void testAdjustForCalendar() {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        LocalDateTime expectedAdjusted = localDateTime.plusHours(-5); // NYC is UTC-5

        LocalDateTime adjusted = DateTimeUtils.adjustForCalendar(localDateTime, calendar, TimeZone.getTimeZone("UTC"));

        assertEquals(expectedAdjusted, adjusted);
    }

    @Test
    void testAdjustForCalendarWithNullCalendar() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        LocalDateTime adjusted = DateTimeUtils.adjustForCalendar(localDateTime, null, TimeZone.getTimeZone("UTC"));

        assertEquals(localDateTime, adjusted);
    }
}
