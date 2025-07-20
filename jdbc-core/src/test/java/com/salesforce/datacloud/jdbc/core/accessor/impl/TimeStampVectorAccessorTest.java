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

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class TimeStampVectorAccessorTest {

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    @InjectSoftAssertions
    private SoftAssertions collector;

    public static final int BASE_YEAR = 2020;
    public static final int NUM_OF_METHODS = 4;

    @Test
    @SneakyThrows
    void testTimestampNanoVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampNanoTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampMicroVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampMicroTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMicroTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimeStampMilliVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimeStampMilliTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampMilliTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampSecVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testTimestampSecTZVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampSecTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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

                assertISOStringLike(stringValue, currentMillis);
            }
        }
        consumer.assertThat().hasNullSeen(0).hasNotNullSeen(NUM_OF_METHODS * values.size());
    }

    @Test
    @SneakyThrows
    void testNulledTimestampVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(extension.createTimeStampSecTZVector(values, "UTC"))) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

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
        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(NUM_OF_METHODS * values.size());
    }

    @SneakyThrows
    @Test
    void testGetTimestampWithDifferentTimeZone() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer);

            Calendar pstCalendar = Calendar.getInstance(TimeZone.getTimeZone("PST"));

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(pstCalendar);
                val stringValue = sut.getString();
                val currentMillis = values.get(i.get());

                pstCalendar.setTimeInMillis(currentMillis);
                long pstMillis = pstCalendar.getTimeInMillis();

                LocalDateTime expectedPST = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(pstMillis),
                        TimeZone.getTimeZone("PST").toZoneId());
                Timestamp expectedTimestamp = Timestamp.valueOf(expectedPST);

                collector.assertThat(timestampValue).isEqualTo(expectedTimestamp);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testSessionTimezoneSupport() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        // Test with session timezone properties
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "America/Los_Angeles");

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, props);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(null);
                val stringValue = sut.getString();
                val currentMillis = values.get(i.get());

                // For naive timestamps with session timezone, should still return literal UTC values
                LocalDateTime expectedUTC = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(currentMillis),
                        TimeZone.getTimeZone("UTC").toZoneId());
                Timestamp expectedTimestamp = Timestamp.valueOf(expectedUTC);

                collector.assertThat(timestampValue).isEqualTo(expectedTimestamp);
                assertNaiveISOStringLike(stringValue, currentMillis);
            }
        }
    }

    @Test
    @SneakyThrows
    void testAvaticaDetectionLogic() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        // Test with session timezone that matches provided calendar
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "America/Los_Angeles");

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, props);

            // Case 1: Calendar matches session timezone (Avatica detection)
            Calendar sessionCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(sessionCalendar);
                val currentMillis = values.get(i.get());

                // Should ignore calendar and return literal UTC values
                LocalDateTime expectedUTC = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(currentMillis),
                        TimeZone.getTimeZone("UTC").toZoneId());
                Timestamp expectedTimestamp = Timestamp.valueOf(expectedUTC);

                collector.assertThat(timestampValue).isEqualTo(expectedTimestamp);
            }
        }
    }

    @Test
    @SneakyThrows
    void testUserProvidedCalendarLogic() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        // Test with session timezone different from provided calendar
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "America/Los_Angeles");

        try (val vector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, props);

            // Case 2: Calendar differs from session timezone (user-provided)
            Calendar userCalendar = Calendar.getInstance(TimeZone.getTimeZone("Europe/London"));

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(userCalendar);
                val currentMillis = values.get(i.get());

                // Should convert from UTC to user calendar timezone
                LocalDateTime utcDateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(currentMillis),
                        TimeZone.getTimeZone("UTC").toZoneId());
                LocalDateTime expectedLondon = utcDateTime
                        .atZone(TimeZone.getTimeZone("UTC").toZoneId())
                        .withZoneSameInstant(
                                TimeZone.getTimeZone("Europe/London").toZoneId())
                        .toLocalDateTime();
                Timestamp expectedTimestamp = Timestamp.valueOf(expectedLondon);

                collector.assertThat(timestampValue).isEqualTo(expectedTimestamp);
            }
        }
    }

    @Test
    @SneakyThrows
    void testTimezoneAwareTimestampHandling() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        // Test with session timezone properties
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "America/Los_Angeles");

        try (val vector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val sut = new TimeStampVectorAccessor(vector, i::get, consumer, props);

            // Test with different calendar timezone
            Calendar londonCalendar = Calendar.getInstance(TimeZone.getTimeZone("Europe/London"));

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val timestampValue = sut.getTimestamp(londonCalendar);
                val stringValue = sut.getString();
                val currentMillis = values.get(i.get());

                // For timezone-aware timestamps, should convert from vector timezone to calendar timezone
                LocalDateTime utcDateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(currentMillis),
                        TimeZone.getTimeZone("UTC").toZoneId());
                LocalDateTime expectedLondon = utcDateTime
                        .atZone(TimeZone.getTimeZone("UTC").toZoneId())
                        .withZoneSameInstant(
                                TimeZone.getTimeZone("Europe/London").toZoneId())
                        .toLocalDateTime();
                Timestamp expectedTimestamp = Timestamp.valueOf(expectedLondon);

                collector.assertThat(timestampValue).isEqualTo(expectedTimestamp);
                // String representation should have 'Z' suffix for timezone-aware
                assertISOStringLike(stringValue, currentMillis);
            }
        }
    }

    @Test
    void testGetSessionTimeZoneWithProperties() {
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "America/New_York");

        TimeZone sessionTz = TimeStampVectorAccessor.getSessionTimeZone(props);
        collector.assertThat(sessionTz).isEqualTo(TimeZone.getTimeZone("America/New_York"));
    }

    @Test
    void testGetSessionTimeZoneWithNullProperties() {
        TimeZone sessionTz = TimeStampVectorAccessor.getSessionTimeZone(null);
        collector.assertThat(sessionTz).isEqualTo(TimeZone.getDefault());
    }

    @Test
    void testGetSessionTimeZoneWithEmptyProperty() {
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "");

        TimeZone sessionTz = TimeStampVectorAccessor.getSessionTimeZone(props);
        collector.assertThat(sessionTz).isEqualTo(TimeZone.getDefault());
    }

    @Test
    void testGetSessionTimeZoneWithInvalidProperty() {
        Properties props = new Properties();
        props.setProperty("querySetting.timezone", "Invalid/Timezone");

        TimeZone sessionTz = TimeStampVectorAccessor.getSessionTimeZone(props);
        collector.assertThat(sessionTz).isEqualTo(TimeZone.getDefault());
    }

    @Test
    @SneakyThrows
    void testHasTimezoneInfoForNaiveTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            boolean hasTimezone = TimeStampVectorAccessor.hasTimezoneInfo(vector);
            collector.assertThat(hasTimezone).isFalse();
        }
    }

    @Test
    @SneakyThrows
    void testHasTimezoneInfoForTimezoneAwareTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            boolean hasTimezone = TimeStampVectorAccessor.hasTimezoneInfo(vector);
            collector.assertThat(hasTimezone).isTrue();
        }
    }

    @Test
    @SneakyThrows
    void testGetTimeZoneForVectorWithTimezone() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoTZVector(values, "America/Los_Angeles")) {
            TimeZone vectorTz = TimeStampVectorAccessor.getTimeZoneForVector(vector);
            collector.assertThat(vectorTz).isEqualTo(TimeZone.getTimeZone("America/Los_Angeles"));
        }
    }

    @Test
    @SneakyThrows
    void testGetTimeZoneForVectorWithoutTimezone() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            TimeZone vectorTz = TimeStampVectorAccessor.getTimeZoneForVector(vector);
            collector.assertThat(vectorTz).isEqualTo(TimeZone.getTimeZone("UTC"));
        }
    }

    @Test
    @SneakyThrows
    void testGetTimeUnitForVector() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);

        try (val vector = extension.createTimeStampNanoVector(values)) {
            TimeUnit timeUnit = TimeStampVectorAccessor.getTimeUnitForVector(vector);
            collector.assertThat(timeUnit).isEqualTo(TimeUnit.NANOSECONDS);
        }

        try (val vector = extension.createTimeStampMicroVector(values)) {
            TimeUnit timeUnit = TimeStampVectorAccessor.getTimeUnitForVector(vector);
            collector.assertThat(timeUnit).isEqualTo(TimeUnit.MICROSECONDS);
        }

        try (val vector = extension.createTimeStampMilliVector(values)) {
            TimeUnit timeUnit = TimeStampVectorAccessor.getTimeUnitForVector(vector);
            collector.assertThat(timeUnit).isEqualTo(TimeUnit.MILLISECONDS);
        }

        try (val vector = extension.createTimeStampSecVector(values)) {
            TimeUnit timeUnit = TimeStampVectorAccessor.getTimeUnitForVector(vector);
            collector.assertThat(timeUnit).isEqualTo(TimeUnit.SECONDS);
        }
    }

    @Test
    @SneakyThrows
    void testStringFormatDifferencesBetweenNaiveAndTimezoneAware() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        List<Integer> monthNumber = getRandomMonthNumber();
        val values = getMilliSecondValues(calendar, monthNumber);
        val consumer = new TestWasNullConsumer(collector);

        // Test naive timestamp string format (no 'Z' suffix)
        try (val naiveVector = extension.createTimeStampNanoVector(values)) {
            val i = new AtomicInteger(0);
            val naiveSut = new TimeStampVectorAccessor(naiveVector, i::get, consumer);

            val naiveString = naiveSut.getString();
            collector.assertThat(naiveString).doesNotEndWith("Z");
        }

        // Test timezone-aware timestamp string format (with 'Z' suffix)
        try (val tzVector = extension.createTimeStampNanoTZVector(values, "UTC")) {
            val i = new AtomicInteger(0);
            val tzSut = new TimeStampVectorAccessor(tzVector, i::get, consumer);

            val tzString = tzSut.getString();
            collector.assertThat(tzString).endsWith("Z");
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

    private void assertISOStringLike(String value, Long millis) {
        collector.assertThat(value).startsWith(getISOString(millis)).matches(".+Z$");
    }

    private void assertNaiveISOStringLike(String value, Long millis) {
        collector.assertThat(value).startsWith(getISOString(millis)).doesNotMatch(".+Z$");
    }

    private String getISOString(Long millis) {
        val formatter = new DateTimeFormatterBuilder().appendInstant(-1).toFormatter();

        return formatter.format(Instant.ofEpochMilli(millis)).replaceFirst("Z$", "");
    }
}
