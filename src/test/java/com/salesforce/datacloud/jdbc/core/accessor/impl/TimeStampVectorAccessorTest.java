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
import java.time.Instant;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
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
                assertISOStringLike(stringValue, currentMillis);
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
                assertISOStringLike(stringValue, currentMillis);
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
                assertISOStringLike(stringValue, currentMillis);
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
                assertISOStringLike(stringValue, currentMillis);
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

    private String getISOString(Long millis) {
        val formatter = new DateTimeFormatterBuilder().appendInstant(-1).toFormatter();

        return formatter.format(Instant.ofEpochMilli(millis)).replaceFirst("Z$", "");
    }
}
