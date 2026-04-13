/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.Constants.ISO_TIME_FORMAT;
import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
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
public class TimeVectorAccessorTest {

    @InjectSoftAssertions
    private SoftAssertions collector;

    public static final String ASIA_BANGKOK = "Asia/Bangkok";

    @RegisterExtension
    public static RootAllocatorTestExtension extension = new RootAllocatorTestExtension();

    private static final int[] edgeCaseVals = {0, 1, 60, 60 * 60, (24 * 60 * 60 - 1)};

    @SneakyThrows
    @Test
    void testTimeNanoVectorGetObjectClass() {

        List<Long> values = generateRandomLongs(TimeUnit.NANOSECONDS);

        try (val vector = extension.createTimeNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Time.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMicroVectorGetObjectClass() {

        List<Long> values = generateRandomLongs(TimeUnit.MICROSECONDS);

        try (val vector = extension.createTimeMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Time.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMilliVectorGetObjectClass() {

        List<Integer> values = generateRandomIntegers(TimeUnit.MILLISECONDS);

        try (val vector = extension.createTimeMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Time.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeSecVectorGetObjectClass() {

        List<Integer> values = generateRandomIntegers(TimeUnit.SECONDS);

        try (val vector = extension.createTimeSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Time.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeNanoVectorGetTime() {

        List<Long> values = generateRandomLongs(TimeUnit.NANOSECONDS);

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
        Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

        try (val vector = extension.createTimeNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.NANOSECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTime(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(calendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(defaultCalendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMicroVectorGetTime() {

        List<Long> values = generateRandomLongs(TimeUnit.MICROSECONDS);

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
        Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

        try (val vector = extension.createTimeMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.MICROSECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTime(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(calendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(defaultCalendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMilliVectorGetTime() {

        List<Integer> values = generateRandomIntegers(TimeUnit.MILLISECONDS);

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
        Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

        try (val vector = extension.createTimeMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(values.get(i.get()));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTime(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(calendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(defaultCalendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeSecVectorGetTime() {

        List<Integer> values = generateRandomIntegers(TimeUnit.SECONDS);

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
        Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

        try (val vector = extension.createTimeSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.SECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTime(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(calendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
                collector
                        .assertThat(sut.getTime(defaultCalendar))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeNanoGetObject() {

        List<Long> values = generateRandomLongs(TimeUnit.NANOSECONDS);

        try (val vector = extension.createTimeNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.NANOSECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector.assertThat(sut.getObject()).isInstanceOf(Time.class);
                collector
                        .assertThat((Time) sut.getObject())
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMicroGetObject() {

        List<Long> values = generateRandomLongs(TimeUnit.MICROSECONDS);

        try (val vector = extension.createTimeMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.MICROSECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector.assertThat(sut.getObject()).isInstanceOf(Time.class);
                collector
                        .assertThat((Time) sut.getObject())
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMilliGetObject() {

        List<Integer> values = generateRandomIntegers(TimeUnit.MILLISECONDS);

        try (val vector = extension.createTimeMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(values.get(i.get()));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector.assertThat(sut.getObject()).isInstanceOf(Time.class);
                collector
                        .assertThat((Time) sut.getObject())
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeSecGetObject() {

        List<Integer> values = generateRandomIntegers(TimeUnit.SECONDS);

        try (val vector = extension.createTimeSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.SECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector.assertThat(sut.getObject()).isInstanceOf(Time.class);
                collector
                        .assertThat((Time) sut.getObject())
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeNanoGetTimestamp() {

        List<Long> values = generateRandomLongs(TimeUnit.NANOSECONDS);

        try (val vector = extension.createTimeNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.NANOSECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTimestamp(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMicroGetTimestamp() {

        List<Long> values = generateRandomLongs(TimeUnit.MICROSECONDS);

        try (val vector = extension.createTimeMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.MICROSECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTimestamp(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMilliGetTimestamp() {

        List<Integer> values = generateRandomIntegers(TimeUnit.MILLISECONDS);

        try (val vector = extension.createTimeMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(values.get(i.get()));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTimestamp(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeSecGetTimestamp() {

        List<Integer> values = generateRandomIntegers(TimeUnit.SECONDS);

        try (val vector = extension.createTimeSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val expectedTime = getExpectedTime(TimeUnit.SECONDS.toMillis(values.get(i.get())));
                val expectedHour = expectedTime.get("hour");
                val expectedMinute = expectedTime.get("minute");
                val expectedSecond = expectedTime.get("second");
                val expectedMilli = expectedTime.get("milli");

                collector
                        .assertThat(sut.getTimestamp(null))
                        .hasHourOfDay(expectedHour)
                        .hasMinute(expectedMinute)
                        .hasSecond(expectedSecond)
                        .hasMillisecond(expectedMilli);
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeNanoGetString() {

        List<Long> values = generateRandomLongs(TimeUnit.NANOSECONDS);

        try (val vector = extension.createTimeNanoVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val currentNanos = values.get(i.get());

                collector.assertThat(stringValue).isEqualTo(getISOString(currentNanos, TimeUnit.NANOSECONDS));
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMicroGetString() {

        List<Long> values = generateRandomLongs(TimeUnit.MICROSECONDS);

        try (val vector = extension.createTimeMicroVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val currentMicros = values.get(i.get());

                collector.assertThat(stringValue).isEqualTo(getISOString(currentMicros, TimeUnit.MICROSECONDS));
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeMilliGetString() {

        List<Integer> values = generateRandomIntegers(TimeUnit.MILLISECONDS);

        try (val vector = extension.createTimeMilliVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val currentMillis = values.get(i.get());

                collector.assertThat(stringValue).isEqualTo(getISOString(currentMillis, TimeUnit.MILLISECONDS));
            }
        }
    }

    @SneakyThrows
    @Test
    void testTimeSecGetString() {

        List<Integer> values = generateRandomIntegers(TimeUnit.SECONDS);

        try (val vector = extension.createTimeSecVector(values)) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val currentSec = values.get(i.get());

                collector.assertThat(stringValue).isEqualTo(getISOString(currentSec, TimeUnit.SECONDS));
            }
        }
    }

    @SneakyThrows
    @Test
    void testNulledOutTimeNanoVectorReturnsNull() {

        List<Long> values = generateRandomLongs(TimeUnit.NANOSECONDS);

        try (val vector = nulledOutVector(extension.createTimeNanoVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
            Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut.getTime(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
            }
        }
    }

    @SneakyThrows
    @Test
    void testNulledOutTimeMicroVectorReturnsNull() {

        List<Long> values = generateRandomLongs(TimeUnit.MICROSECONDS);

        try (val vector = nulledOutVector(extension.createTimeMicroVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
            Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut.getTime(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
            }
        }
    }

    @SneakyThrows
    @Test
    void testNulledOutTimeMilliVectorReturnsNull() {

        List<Integer> values = generateRandomIntegers(TimeUnit.MILLISECONDS);

        try (val vector = nulledOutVector(extension.createTimeMilliVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
            Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut.getTime(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
            }
        }
    }

    @SneakyThrows
    @Test
    void testNulledOutTimeSecVectorReturnsNull() {

        List<Integer> values = generateRandomIntegers(TimeUnit.SECONDS);

        try (val vector = nulledOutVector(extension.createTimeSecVector(values))) {
            val i = new AtomicInteger(0);
            val sut = new TimeVectorAccessor(vector, i::get);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
            Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut.getTime(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
            }
        }
    }

    private static List<Long> generateRandomLongs(TimeUnit timeUnit) {
        int rightLimit = 86400;
        val i = new AtomicInteger(0);
        List<Long> result = new ArrayList<>();

        for (; i.get() < edgeCaseVals.length; i.incrementAndGet()) {
            switch (timeUnit) {
                case NANOSECONDS:
                    result.add(ThreadLocalRandom.current().nextLong(rightLimit * 1_000_000_000L));
                    result.add(edgeCaseVals[i.get()] * 1_000_000_000L);
                    break;
                case MICROSECONDS:
                    result.add(ThreadLocalRandom.current().nextLong(rightLimit * 1_000_000L));
                    result.add(edgeCaseVals[i.get()] * 1_000_000L);
                    break;
                default:
            }
        }

        return result;
    }

    private static List<Integer> generateRandomIntegers(TimeUnit timeUnit) {
        int rightLimit = 86400;
        val i = new AtomicInteger(0);
        List<Integer> result = new ArrayList<>();
        Random rnd = new Random();

        for (; i.get() < edgeCaseVals.length; i.incrementAndGet()) {
            switch (timeUnit) {
                case MILLISECONDS:
                    result.add(rnd.nextInt(rightLimit * 1_000));
                    result.add(edgeCaseVals[i.get()] * 1_000);
                    break;
                case SECONDS:
                    result.add(rnd.nextInt(rightLimit));
                    result.add(edgeCaseVals[i.get()]);
                    break;
                default:
            }
        }

        return result;
    }

    private Map<String, Integer> getExpectedTime(long epochMilli) {
        final ZoneId zoneId = TimeZone.getTimeZone("UTC").toZoneId();
        final Instant instant = Instant.ofEpochMilli(epochMilli);
        final ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);

        final int expectedHour = zdt.getHour();
        final int expectedMinute = zdt.getMinute();
        final int expectedSecond = zdt.getSecond();
        final int expectedMillisecond = (int) TimeUnit.NANOSECONDS.toMillis(zdt.getNano());

        return ImmutableMap.of(
                "hour", expectedHour, "minute", expectedMinute, "second", expectedSecond, "milli", expectedMillisecond);
    }

    private String getISOString(Long value, TimeUnit unit) {
        Long adjustedNanos;
        switch (unit) {
            case NANOSECONDS:
                adjustedNanos = value;
                break;
            case MICROSECONDS:
                adjustedNanos = value * 1_000;
                break;
            default:
                adjustedNanos = value;
        }

        val localTime = LocalTime.ofNanoOfDay(adjustedNanos);
        val result = localTime.format(DateTimeFormatter.ofPattern(ISO_TIME_FORMAT));
        return result;
    }

    private String getISOString(Integer value, TimeUnit unit) {
        Integer adjustedSeconds;
        switch (unit) {
            case MILLISECONDS:
                adjustedSeconds = value / 1_000;
                break;
            case SECONDS:
                adjustedSeconds = value;
                break;
            default:
                adjustedSeconds = value;
        }

        val localTime = LocalTime.ofSecondOfDay(adjustedSeconds);
        val result = localTime.format(DateTimeFormatter.ofPattern(ISO_TIME_FORMAT));
        return result;
    }
}
