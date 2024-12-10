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

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.appendDates;
import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;
import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import com.salesforce.datacloud.jdbc.util.TestWasNullConsumer;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
public class DateVectorAccessorTest {

    @InjectSoftAssertions
    private SoftAssertions collector;

    public static final String ASIA_BANGKOK = "Asia/Bangkok";
    public static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    private static final int expectedZero = 0;

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    private static final List<Long> values = Stream.of(
                    1625702400000L, // 2021-07-08 00:00:00 UTC, 2021-07-07 17:00:00 PT
                    1625788800000L, // 2021-07-09 00:00:00 UTC, 2021-07-08 17:00:00 PT
                    0L, // 1970-01-01 00:00:00 UTC, 1969-12-31 16:00:00 PT
                    -1625702400000L, // 1918-06-27 00:00:00 UTC, 1918-06-26 17:00:00 PT
                    -601689600000L) // 1950-12-08 00:00:00 UTC, 1950-12-07 16:00:00 PT
            .collect(Collectors.toList());

    private static final int expectedNulls =
            (int) values.stream().filter(Objects::isNull).count();

    private static final int expectedNonNulls = values.size() - expectedNulls;

    @SneakyThrows
    @Test
    void testDateDayVectorGetObjectClass() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Date.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetObjectClass() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Date.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetDateReturnsUTCDate() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedDate = getExpectedDateForTimeZone(values.get(i.get()), UTC_ZONE_ID);
                val expectedYear = expectedDate.get("year");
                val expectedMonth = expectedDate.get("month");
                val expectedDay = expectedDate.get("day");

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector
                        .assertThat(sut.getDate(null))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
                collector
                        .assertThat(sut.getDate(calendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
                collector
                        .assertThat(sut.getDate(defaultCalendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls * 3).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetDateReturnsUTCDate() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedDate = getExpectedDateForTimeZone(values.get(i.get()), UTC_ZONE_ID);
                val expectedYear = expectedDate.get("year");
                val expectedMonth = expectedDate.get("month");
                val expectedDay = expectedDate.get("day");

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector
                        .assertThat(sut.getDate(null))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
                collector
                        .assertThat(sut.getDate(calendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
                collector
                        .assertThat(sut.getDate(defaultCalendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls * 3).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetObjectReturnsUTCDate() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedDate = getExpectedDateForTimeZone(values.get(i.get()), UTC_ZONE_ID);
                val expectedYear = expectedDate.get("year");
                val expectedMonth = expectedDate.get("month");
                val expectedDay = expectedDate.get("day");

                collector.assertThat(sut.getObject()).isInstanceOf(Date.class);
                collector
                        .assertThat((Date) sut.getObject())
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls * 2).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetObjectReturnsUTCDate() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedDate = getExpectedDateForTimeZone(values.get(i.get()), UTC_ZONE_ID);
                val expectedYear = expectedDate.get("year");
                val expectedMonth = expectedDate.get("month");
                val expectedDay = expectedDate.get("day");

                collector.assertThat(sut.getObject()).isInstanceOf(Date.class);
                collector
                        .assertThat((Date) sut.getObject())
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls * 2).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetTimestampReturnsUTCTimestampAtMidnight() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedDate = getExpectedDateForTimeZone(values.get(i.get()), UTC_ZONE_ID);
                val expectedYear = expectedDate.get("year");
                val expectedMonth = expectedDate.get("month");
                val expectedDay = expectedDate.get("day");

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector
                        .assertThat(sut.getTimestamp(null))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero)
                        .hasMillisecond(expectedZero);
                collector
                        .assertThat(sut.getTimestamp(calendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero);
                collector
                        .assertThat(sut.getTimestamp(defaultCalendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls * 3).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetTimestampReturnsUTCTimestampAtMidnight() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedDate = getExpectedDateForTimeZone(values.get(i.get()), UTC_ZONE_ID);
                val expectedYear = expectedDate.get("year");
                val expectedMonth = expectedDate.get("month");
                val expectedDay = expectedDate.get("day");

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector
                        .assertThat(sut.getTimestamp(null))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero);
                collector
                        .assertThat(sut.getTimestamp(calendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero);
                collector
                        .assertThat(sut.getTimestamp(defaultCalendar))
                        .hasYear(expectedYear)
                        .hasMonth(expectedMonth)
                        .hasDayOfMonth(expectedDay)
                        .hasHourOfDay(expectedZero)
                        .hasMinute(expectedZero)
                        .hasSecond(expectedZero);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls * 3).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetString() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val expected = getISOString(values.get(i.get()));
                collector.assertThat(stringValue).isEqualTo(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetString() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val expected = getISOString(values.get(i.get()));
                collector.assertThat(stringValue).isEqualTo(expected);
            }
        }

        consumer.assertThat().hasNotNullSeen(expectedNonNulls).hasNullSeen(expectedNulls);
    }

    @SneakyThrows
    @Test
    void testNulledOutDateDayVectorReturnsNull() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(appendDates(values, extension.createDateDayVector()))) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector.assertThat(sut.getDate(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(values.size() * 4);
    }

    @SneakyThrows
    @Test
    void testNulledOutDateMilliVectorReturnsNull() {
        val consumer = new TestWasNullConsumer(collector);

        try (val vector = nulledOutVector(appendDates(values, extension.createDateMilliVector()))) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get, consumer);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector.assertThat(sut.getDate(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
            }
        }

        consumer.assertThat().hasNotNullSeen(0).hasNullSeen(values.size() * 4);
    }

    private Map<String, Integer> getExpectedDateForTimeZone(long epochMilli, ZoneId zoneId) {
        final Instant instant = Instant.ofEpochMilli(epochMilli);
        final ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);

        final int expectedYear = zdt.getYear();
        final int expectedMonth = zdt.getMonthValue();
        final int expectedDay = zdt.getDayOfMonth();

        return ImmutableMap.of("year", expectedYear, "month", expectedMonth, "day", expectedDay);
    }

    private String getISOString(Long millis) {
        val epochDays = millis / MILLIS_PER_DAY;
        LocalDate localDate = LocalDate.ofEpochDay(epochDays);
        return localDate.format(DateTimeFormatter.ISO_DATE);
    }
}
