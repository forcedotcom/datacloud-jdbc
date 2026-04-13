/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class TimeStampVectorAccessorTest {

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @InjectSoftAssertions
    private SoftAssertions collector;

    private static TimeZone originalTimeZone;

    @BeforeAll
    static void pinTimezone() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @AfterAll
    static void restoreTimezone() {
        TimeZone.setDefault(originalTimeZone);
    }

    public static final int BASE_YEAR = 2020;

    @Test
    @SneakyThrows
    void testTimestampNanoVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.getAndIncrement()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimestampNanoTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampTZStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimestampMicroVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimestampMicroTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampMicroTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampTZStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimeStampMilliVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimeStampMilliTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = values.get(i.get());

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampTZStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimestampSecVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = (values.get(i.get()) / 1000) * 1000;

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                assertTimestampStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimestampSecTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampSecTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();
                val currentNumber = monthNumber.get(i.get());
                val currentMillis = (values.get(i.get()) / 1000) * 1000;

                collector
                        .assertThat(timestampValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(dateValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);
                collector
                        .assertThat(timeValue)
                        .hasYear(BASE_YEAR + currentNumber)
                        .hasMonth(currentNumber + 1)
                        .hasDayOfMonth(currentNumber)
                        .hasHourOfDay(currentNumber)
                        .hasMinute(currentNumber)
                        .hasSecond(currentNumber);

                assertTimestampTZStringFormat(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testNulledTimestampVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = nulledOutVector(extension.createTimeStampSecTZVector(values, "UTC"))) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(calendar);
                val dateValue = sut.getDate(calendar);
                val timeValue = sut.getTime(calendar);
                val stringValue = sut.getString();

                collector.assertThat(timestampValue).isNull();
                collector.assertThat(dateValue).isNull();
                collector.assertThat(timeValue).isNull();
                collector.assertThat(stringValue).isNull();
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetTimestampCalendarInterpretsLiteralInCalendarTimezone() {
        // For naive TIMESTAMP, getTimestamp(Calendar) interprets the stored literal
        // as being in the Calendar's timezone (matching Postgres JDBC behavior).
        // UTC calendar → literal treated as UTC → epoch = raw stored epoch.
        // GMT-8 calendar → literal treated as GMT-8 → epoch = raw stored epoch + 8h.
        Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(utcCalendar, monthNumber);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            // Use GMT-8:00 (fixed offset, no DST) for deterministic assertions
            Calendar gmtMinus8 = Calendar.getInstance(TimeZone.getTimeZone("GMT-8:00"));

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val withUtc = sut.getTimestamp(utcCalendar);
                val withGmtMinus8 = sut.getTimestamp(gmtMinus8);
                val currentMillis = values.get(i.get());

                // UTC calendar: literal interpreted as UTC → same epoch as how values were created
                collector.assertThat(withUtc.getTime()).isEqualTo(currentMillis);

                // GMT-8 calendar: literal treated as GMT-8 → epoch = literal + 8h in UTC
                collector.assertThat(withGmtMinus8.getTime()).isEqualTo(currentMillis + 8 * 3600_000L);
                assertTimestampStringFormat(sut.getString(), currentMillis);
            }
        }
    }

    @SneakyThrows
    @Test
    void testGetTimestampCalendarEpochForKnownValue() {
        // Verifies exact epoch millis for getTimestamp(Calendar) against a known naive TIMESTAMP.
        // Literal stored: 2024-03-15 12:00:00 (encoded as UTC epoch = 2024-03-15T12:00:00Z)
        // getTimestamp(utcCal):   literal treated as UTC  → epoch = 2024-03-15T12:00:00Z
        // getTimestamp(tokyoCal): literal treated as JST  → epoch = 2024-03-15T03:00:00Z (UTC+9)
        long literalAsUtcMs = Instant.parse("2024-03-15T12:00:00Z").toEpochMilli();
        long literalAsTokyo = Instant.parse("2024-03-15T03:00:00Z").toEpochMilli(); // 12:00 JST


        try (val vector = extension.createTimeStampMicroVector(ImmutableList.of(literalAsUtcMs))) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            Calendar tokyoCal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));

            assertThat(sut.getTimestamp(utcCal).getTime()).isEqualTo(literalAsUtcMs);
            assertThat(sut.getTimestamp(tokyoCal).getTime()).isEqualTo(literalAsTokyo);
        }
    }

    private List<Long> getMilliSecondValues(Calendar calendar, List<Integer> monthNumber) {
        List<Long> result = new ArrayList<>();
        for (int currentNumber : monthNumber) {
            calendar.set(
                    BASE_YEAR + currentNumber,
                    currentNumber,
                    currentNumber,
                    currentNumber,
                    currentNumber,
                    currentNumber);
            result.add(calendar.getTimeInMillis());
        }
        return result;
    }

    private List<Integer> getRandomMonthNumber() {
        Random rand = new Random();
        int valA = rand.nextInt(10) + 1;
        int valB = rand.nextInt(10) + 1;
        int valC = rand.nextInt(10) + 1;
        return ImmutableList.of(valA, valB, valC);
    }

    /**
     * Assert string format for TIMESTAMP (naive - no timezone in metadata).
     * Format: "yyyy-MM-dd HH:mm:ss.SSSSSS" (no offset)
     */
    private void assertTimestampStringFormat(String value, Long millis) {
        collector.assertThat(value).isNotNull();
        collector.assertThat(value).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}$");
        collector.assertThat(value).hasSize(26);
    }

    /**
     * Assert string format for TIMESTAMPTZ (with timezone in metadata).
     * Format: "yyyy-MM-dd HH:mm:ss.SSSSSS XXX" (with offset like +00:00)
     */
    private void assertTimestampTZStringFormat(String value, Long millis) {
        collector.assertThat(value).isNotNull();
        collector.assertThat(value).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6} [+-]\\d{2}:\\d{2}$");
        collector.assertThat(value).hasSizeGreaterThanOrEqualTo(32);
    }

    private void assertISOStringLike(String value, Long millis) {
        assertTimestampStringFormat(value, millis);
    }

    private String getISOString(Long millis) {
        val formatter = new DateTimeFormatterBuilder().appendInstant(-1).toFormatter();
        return formatter.format(Instant.ofEpochMilli(millis)).replaceFirst("Z$", "");
    }

    @Test
    @SneakyThrows
    void testGetObjectWithInstantClassThrowsForTimestampTZ() {
        List<Integer> monthNumber = getRandomMonthNumber();
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            assertThatThrownBy(() -> sut.getObject(Instant.class)).isInstanceOf(SQLFeatureNotSupportedException.class);
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithOffsetDateTimeClass() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                OffsetDateTime odt = sut.getObject(OffsetDateTime.class);
                collector.assertThat(odt).isNotNull();
                collector.assertThat(odt.toInstant().toEpochMilli()).isEqualTo(values.get(i.get()));
            }
        }

    }

    @Test
    @SneakyThrows
    void testGetObjectWithZonedDateTimeClass() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                ZonedDateTime zdt = sut.getObject(ZonedDateTime.class);
                collector.assertThat(zdt).isNotNull();
                collector.assertThat(zdt.toInstant().toEpochMilli()).isEqualTo(values.get(i.get()));
            }
        }

    }

    @Test
    @SneakyThrows
    void testGetObjectWithLocalDateTimeClass() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                LocalDateTime ldt = sut.getObject(LocalDateTime.class);
                collector.assertThat(ldt).isNotNull();
            }
        }

    }

    @Test
    @SneakyThrows
    void testGetObjectWithTimestampClass() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                Timestamp ts = sut.getObject(Timestamp.class);
                collector.assertThat(ts).isNotNull();
            }
        }

    }

    @Test
    @SneakyThrows
    void testGetObjectClassReturnsTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            collector.assertThat(sut.getObjectClass()).isEqualTo(Timestamp.class);
        }
    }

    @Test
    @SneakyThrows
    void testGetTimestampWithNullCalendar() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        // Test naive TIMESTAMP with null calendar
        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                Timestamp ts = sut.getTimestamp(null);
                collector.assertThat(ts).isNotNull();
            }
        }

        // Test TIMESTAMPTZ with null calendar
        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                Timestamp ts = sut.getTimestamp(null);
                collector.assertThat(ts).isNotNull();
            }
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithAllTypesForNaiveTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                Instant instant = sut.getObject(Instant.class);
                OffsetDateTime odt = sut.getObject(OffsetDateTime.class);
                ZonedDateTime zdt = sut.getObject(ZonedDateTime.class);
                LocalDateTime ldt = sut.getObject(LocalDateTime.class);
                Timestamp ts = sut.getObject(Timestamp.class);

                collector.assertThat(instant).isNotNull();
                collector.assertThat(odt).isNotNull();
                collector.assertThat(zdt).isNotNull();
                collector.assertThat(ldt).isNotNull();
                collector.assertThat(ts).isNotNull();

                collector.assertThat(instant.toEpochMilli()).isEqualTo(values.get(i.get()));
                collector.assertThat(odt.toInstant().toEpochMilli()).isEqualTo(values.get(i.get()));
                collector.assertThat(zdt.toInstant().toEpochMilli()).isEqualTo(values.get(i.get()));
            }
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithAllTypesForTimestampTZ() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "America/Los_Angeles")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                OffsetDateTime odt = sut.getObject(OffsetDateTime.class);
                ZonedDateTime zdt = sut.getObject(ZonedDateTime.class);
                LocalDateTime ldt = sut.getObject(LocalDateTime.class);
                Timestamp ts = sut.getObject(Timestamp.class);

                collector.assertThat(odt).isNotNull();
                collector.assertThat(zdt).isNotNull();
                collector.assertThat(ldt).isNotNull();
                collector.assertThat(ts).isNotNull();

                collector.assertThat(odt.toInstant().toEpochMilli()).isEqualTo(values.get(i.get()));
                collector.assertThat(zdt.toInstant().toEpochMilli()).isEqualTo(values.get(i.get()));

                collector.assertThat(zdt.getZone().getId()).isEqualTo("America/Los_Angeles");
            }
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithNullValues() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = nulledOutVector(extension.createTimeStampMilliTZVector(values, "UTC"))) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut.getObject(OffsetDateTime.class)).isNull();
                collector.assertThat(sut.getObject(ZonedDateTime.class)).isNull();
                collector.assertThat(sut.getObject(LocalDateTime.class)).isNull();
                collector.assertThat(sut.getObject(Timestamp.class)).isNull();
                collector.assertThat(sut.<Date>getObject(Date.class)).isNull();
                collector.assertThat(sut.<Time>getObject(Time.class)).isNull();
                collector.assertThat(sut.getObject(String.class)).isNull();
            }
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithUnsupportedType() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            assertThatThrownBy(() -> sut.getObject(Integer.class)).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(() -> sut.getObject(Long.class)).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(() -> sut.getObject(Double.class)).isInstanceOf(SQLFeatureNotSupportedException.class);
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithNullTypeThrows() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        // Naive TIMESTAMP
        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);
            assertThatThrownBy(() -> sut.getObject((Class<?>) null))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("must not be null");
        }

        // TIMESTAMPTZ
        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);
            assertThatThrownBy(() -> sut.getObject((Class<?>) null))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("must not be null");
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithDateTimeStringTypesForNaiveTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                Date date = sut.getObject(Date.class);
                Time time = sut.getObject(Time.class);
                String str = sut.getObject(String.class);

                collector.assertThat(date).isNotNull();
                collector.assertThat(time).isNotNull();
                collector.assertThat(str).isNotNull();
                collector.assertThat(str).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}$");
            }
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithUnsupportedTypeForNaiveTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            assertThatThrownBy(() -> sut.getObject(Integer.class)).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(() -> sut.getObject(Long.class)).isInstanceOf(SQLFeatureNotSupportedException.class);
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectWithNullValuesForNaiveTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = nulledOutVector(extension.createTimeStampMilliVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut.getObject(Instant.class)).isNull();
                collector.assertThat(sut.getObject(OffsetDateTime.class)).isNull();
                collector.assertThat(sut.getObject(ZonedDateTime.class)).isNull();
                collector.assertThat(sut.getObject(LocalDateTime.class)).isNull();
                collector.assertThat(sut.getObject(Timestamp.class)).isNull();
                collector.assertThat(sut.<Date>getObject(Date.class)).isNull();
                collector.assertThat(sut.<Time>getObject(Time.class)).isNull();
                collector.assertThat(sut.getObject(String.class)).isNull();
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimestampTZWithInvalidArrowMetadataTimezone() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        // Invalid timezone string → extractArrowMetadataZone returns null → systemDefault fallback
        try (val vector = extension.createTimeStampMilliTZVector(values, "INVALID_TZ")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                Timestamp ts = sut.getTimestamp(null);
                collector.assertThat(ts).isNotNull();
            }
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectClassReturnsTimestampForNaiveAccessor() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get);
            collector.assertThat(sut.getObjectClass()).isEqualTo(Timestamp.class);
        }
    }

    @Test
    @SneakyThrows
    void testGetObjectClassReturnsTimestampForTZAccessor() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);


        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampTZVectorAccessor(vector, i::get);
            collector.assertThat(sut.getObjectClass()).isEqualTo(Timestamp.class);
        }
    }
}
