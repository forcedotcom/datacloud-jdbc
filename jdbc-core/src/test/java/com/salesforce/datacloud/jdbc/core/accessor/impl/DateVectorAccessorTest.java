/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core.accessor.impl;

import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.appendDates;
import static com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension.nulledOutVector;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.accessor.SoftAssertions;
import com.salesforce.datacloud.jdbc.util.RootAllocatorTestExtension;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
    private static final long MILLIS_PER_DAY = 86_400_000L;

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

    @SneakyThrows
    @Test
    public void testDateDayVectorGetObjectClass() {

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Date.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetObjectClass() {

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                collector.assertThat(sut).hasObjectClass(Date.class);
            }
        }
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetDateReturnsUTCDate() {

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

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
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetDateReturnsUTCDate() {

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

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
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetObjectReturnsUTCDate() {

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

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
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetObjectReturnsUTCDate() {

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

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
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetTimestampReturnsUTCTimestampAtMidnight() {

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

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
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetTimestampReturnsUTCTimestampAtMidnight() {

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

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
    }

    @SneakyThrows
    @Test
    void testDateMilliVectorGetString() {

        try (val vector = appendDates(values, extension.createDateMilliVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val expected = getISOString(values.get(i.get()));
                collector.assertThat(stringValue).isEqualTo(expected);
            }
        }
    }

    @SneakyThrows
    @Test
    void testDateDayVectorGetString() {

        try (val vector = appendDates(values, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {
                val stringValue = sut.getString();
                val expected = getISOString(values.get(i.get()));
                collector.assertThat(stringValue).isEqualTo(expected);
            }
        }
    }

    @SneakyThrows
    @Test
    void testNulledOutDateDayVectorReturnsNull() {

        try (val vector = nulledOutVector(appendDates(values, extension.createDateDayVector()))) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector.assertThat(sut.getDate(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
                collector.assertThat(sut.getObject(LocalDate.class)).isNull();
            }
        }
    }

    @SneakyThrows
    @Test
    void testNulledOutDateMilliVectorReturnsNull() {

        try (val vector = nulledOutVector(appendDates(values, extension.createDateMilliVector()))) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            for (; i.get() < vector.getValueCount(); i.incrementAndGet()) {

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ASIA_BANGKOK));
                Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());

                collector.assertThat(sut.getDate(defaultCalendar)).isNull();
                collector.assertThat(sut.getTimestamp(calendar)).isNull();
                collector.assertThat(sut.getObject()).isNull();
                collector.assertThat(sut.getString()).isNull();
                collector.assertThat(sut.getObject(LocalDate.class)).isNull();
            }
        }
    }

    /** Proleptic Gregorian dates (as stored by Arrow/Hyper) paired with the server's text form. */
    private static final Map<String, String> HISTORICAL_DATES = ImmutableMap.<String, String>builder()
            .put("0001-01-01", "0001-01-01")
            .put("1500-02-28", "1500-02-28")
            .put("1582-10-04", "1582-10-04")
            .put("1582-10-15", "1582-10-15")
            .put("0000-12-31", "0001-12-31 BC")
            .put("0000-01-01", "0001-01-01 BC")
            .put("-0001-01-01", "0002-01-01 BC")
            .put("1969-12-31", "1969-12-31")
            .put("2000-02-29", "2000-02-29")
            .put("9999-12-31", "9999-12-31")
            .build();

    private static final List<String> HISTORICAL_TIME_ZONES =
            Stream.of("UTC", "America/Los_Angeles", "Asia/Kolkata").collect(Collectors.toList());

    @SneakyThrows
    @Test
    void testHistoricalDatesAreLosslessAcrossVectorsAndTimeZones() {
        val original = TimeZone.getDefault();
        try {
            for (val tz : HISTORICAL_TIME_ZONES) {
                TimeZone.setDefault(TimeZone.getTimeZone(tz));
                val millis = HISTORICAL_DATES.keySet().stream()
                        .map(d -> LocalDate.parse(d).toEpochDay() * MILLIS_PER_DAY)
                        .collect(Collectors.toList());
                val expectedText = new ArrayList<>(HISTORICAL_DATES.values());

                try (val dayVector = appendDates(millis, extension.createDateDayVector());
                        val milliVector = appendDates(millis, extension.createDateMilliVector())) {
                    val row = new AtomicInteger(0);
                    val daySut = new DateVectorAccessor(dayVector, row::get);
                    val milliSut = new DateVectorAccessor(milliVector, row::get);
                    for (; row.get() < millis.size(); row.incrementAndGet()) {
                        val i = row.get();
                        val ctx = tz + " " + expectedText.get(i);
                        val epochDay = millis.get(i) / MILLIS_PER_DAY;
                        for (val sut : Arrays.asList(daySut, milliSut)) {
                            collector.assertThat(sut.getString()).as(ctx).isEqualTo(expectedText.get(i));
                            collector
                                    .assertThat(sut.getObject(LocalDate.class))
                                    .as(ctx)
                                    .isEqualTo(LocalDate.ofEpochDay(epochDay));
                            // java.sql.Date keeps the calendar fields (including the era) of the SQL date at
                            // local midnight in the default time zone.
                            val expectedDate = LocalDate.ofEpochDay(epochDay);
                            val actual = Calendar.getInstance();
                            actual.setTime(sut.getDate(null));
                            val bc = expectedDate.getYear() <= 0;
                            val ctxDate = ctx + " java.sql.Date";
                            collector
                                    .assertThat(actual.get(Calendar.ERA))
                                    .as(ctxDate)
                                    .isEqualTo(bc ? GregorianCalendar.BC : GregorianCalendar.AD);
                            collector
                                    .assertThat(actual.get(Calendar.YEAR))
                                    .as(ctxDate)
                                    .isEqualTo(bc ? 1 - expectedDate.getYear() : expectedDate.getYear());
                            collector
                                    .assertThat(actual.get(Calendar.MONTH) + 1)
                                    .as(ctxDate)
                                    .isEqualTo(expectedDate.getMonthValue());
                            collector
                                    .assertThat(actual.get(Calendar.DAY_OF_MONTH))
                                    .as(ctxDate)
                                    .isEqualTo(expectedDate.getDayOfMonth());
                            collector
                                    .assertThat(actual.get(Calendar.HOUR_OF_DAY))
                                    .as(ctxDate)
                                    .isZero();
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @SneakyThrows
    @Test
    void testGetStringIgnoresDefaultLocaleDigits() {
        val original = Locale.getDefault();
        val millis = Stream.of("2024-03-05", "0000-01-01")
                .map(d -> LocalDate.parse(d).toEpochDay() * MILLIS_PER_DAY)
                .collect(Collectors.toList());
        try (val vector = appendDates(millis, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);
            for (val tag : Arrays.asList("ar-SA-u-nu-arab", "hi-IN-u-nu-deva", "th-TH-u-nu-thai")) {
                Locale.setDefault(Locale.forLanguageTag(tag));
                i.set(0);
                collector.assertThat(sut.getString()).as(tag).isEqualTo("2024-03-05");
                i.set(1);
                collector.assertThat(sut.getString()).as(tag).isEqualTo("0001-01-01 BC");
            }
        } finally {
            Locale.setDefault(original);
        }
    }

    /** java.sql.Date has no 1582-10-05..14 (skipped by the Julian-to-Gregorian cutover); pin the rollover. */
    @SneakyThrows
    @Test
    void testGetDateForCutoverGapRollsForward() {
        val millis = Collections.singletonList(LocalDate.parse("1582-10-05").toEpochDay() * MILLIS_PER_DAY);
        try (val vector = appendDates(millis, extension.createDateDayVector())) {
            val sut = new DateVectorAccessor(vector, () -> 0);
            collector.assertThat(sut.getString()).isEqualTo("1582-10-05");
            collector.assertThat(sut.getObject(LocalDate.class)).isEqualTo(LocalDate.parse("1582-10-05"));
            collector.assertThat(sut.getDate(null).toLocalDate()).isEqualTo(LocalDate.parse("1582-10-15"));
        }
    }

    @SneakyThrows
    @Test
    void testGetObjectWithClassFallsBackToDefaultConversions() {
        val millis = Stream.of("2000-02-29", "0000-01-01")
                .map(d -> LocalDate.parse(d).toEpochDay() * MILLIS_PER_DAY)
                .collect(Collectors.toList());

        try (val vector = appendDates(millis, extension.createDateDayVector())) {
            val i = new AtomicInteger(0);
            val sut = new DateVectorAccessor(vector, i::get);

            collector.assertThat(sut.getObject(String.class)).isEqualTo("2000-02-29");
            collector.assertThat(sut.getObject(Date.class)).isEqualTo(Date.valueOf("2000-02-29"));
            collector.assertThat(sut.getObject(java.util.Date.class)).isEqualTo(Date.valueOf("2000-02-29"));
            collector
                    .assertThatThrownBy(() -> sut.getObject(Integer.class))
                    .isInstanceOf(SQLFeatureNotSupportedException.class);
            collector.assertThatThrownBy(() -> sut.getObject((Class<?>) null)).isInstanceOf(SQLException.class);

            i.incrementAndGet();
            collector.assertThat(sut.getObject(String.class)).isEqualTo("0001-01-01 BC");
            collector.assertThat(sut.getObject(LocalDate.class)).isEqualTo(LocalDate.of(0, 1, 1));
        }
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
