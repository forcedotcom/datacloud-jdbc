/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for timezone and timestamp handling in DataCloud JDBC driver.
 *
 * Tests the timezone precedence order:
 * 1. Calendar parameter (per-call setting)
 * 2. Arrow metadata timezone (from Hyper - TIMESTAMPTZ columns)
 * 3. Session timezone (query setting time_zone)
 * 4. System default
 */
@ExtendWith(LocalHyperTestBase.class)
public class TimeZoneIntegrationTest {

    @Test
    @SneakyThrows
    public void testSessionTimezoneResolution() {
        // Naive TIMESTAMP preserves the literal wall-clock value regardless of session timezone.
        // TIMESTAMP '2024-03-15 10:00:00.123456' always returns toString() = "2024-03-15 10:00:00.123456"
        // regardless of what session timezone is set.
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "America/New_York");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // LocalDateTime always returns the raw literal (no TZ context)
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    assertThat(ldt.getYear()).isEqualTo(2024);
                    assertThat(ldt.getMonthValue()).isEqualTo(3);
                    assertThat(ldt.getDayOfMonth()).isEqualTo(15);
                    assertThat(ldt.getHour()).isEqualTo(10);
                    assertThat(ldt.getMinute()).isEqualTo(0);
                    assertThat(ldt.getSecond()).isEqualTo(0);
                    assertThat(ldt.getNano()).isEqualTo(123456000);

                    // getTimestamp() preserves the literal: toString() = "2024-03-15 10:00:00.123456"
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).isNotNull();
                    assertThat(ts.toString()).startsWith("2024-03-15 10:00:00.123456");

                    // getString() returns the literal wall-clock string (no timezone offset)
                    String tsString = rs.getString("test_timestamp");
                    assertThat(tsString).isEqualTo("2024-03-15 10:00:00.123456");
                    assertThat(tsString).doesNotMatch(".*\\s[+-]\\d{2}:\\d{2}$");

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testJDBC42JavaTimeTypes() {
        // Test JDBC 4.2 java.time types support with actual value assertions
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test LocalDateTime - naive, no timezone - should match literal value
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    assertThat(ldt).isNotNull();
                    assertThat(ldt.getYear()).isEqualTo(2024);
                    assertThat(ldt.getMonthValue()).isEqualTo(3);
                    assertThat(ldt.getDayOfMonth()).isEqualTo(15);
                    assertThat(ldt.getHour()).isEqualTo(14); // Should be 14, not 7!
                    assertThat(ldt.getMinute()).isEqualTo(30);
                    assertThat(ldt.getSecond()).isEqualTo(45);
                    assertThat(ldt.getNano()).isEqualTo(123456000); // microseconds to nanos

                    // Test Instant - for naive TIMESTAMP the accessor encodes the literal as UTC
                    Instant instant = rs.getObject("test_timestamp", Instant.class);
                    assertThat(instant).isEqualTo(Instant.parse("2024-03-15T14:30:45.123456Z"));

                    // Test OffsetDateTime - naive TIMESTAMP is surfaced at UTC+00:00 offset
                    OffsetDateTime odt = rs.getObject("test_timestamp", OffsetDateTime.class);
                    assertThat(odt.getOffset()).isEqualTo(ZoneOffset.UTC);
                    assertThat(odt.getHour()).isEqualTo(14); // literal hour preserved
                    assertThat(odt.toLocalDateTime()).isEqualTo(ldt);

                    // Test ZonedDateTime - naive TIMESTAMP is surfaced in UTC zone
                    ZonedDateTime zdt = rs.getObject("test_timestamp", ZonedDateTime.class);
                    assertThat(zdt.getZone()).isEqualTo(ZoneId.of("UTC"));
                    assertThat(zdt.getHour()).isEqualTo(14); // literal hour preserved
                    assertThat(zdt.toLocalDateTime()).isEqualTo(ldt);

                    // Test Timestamp - should match literal value
                    Timestamp ts = rs.getObject("test_timestamp", Timestamp.class);
                    assertThat(ts).isNotNull();
                    assertThat(ts.toString()).startsWith("2024-03-15 14:30:45.123456");

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testTimezonePrecedence() {
        // Test that Calendar parameter overrides session timezone
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "UTC");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 12:00:00' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // LocalDateTime always returns the raw literal — hour must be 12
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    assertThat(ldt).isNotNull();
                    assertThat(ldt.getHour()).isEqualTo(12);
                    assertThat(ldt.getMinute()).isEqualTo(0);
                    assertThat(ldt.getSecond()).isEqualTo(0);

                    // Get with default (session timezone = UTC)
                    Timestamp tsDefault = rs.getTimestamp("test_timestamp");
                    assertThat(tsDefault).isNotNull();

                    // Get with Calendar override (Asia/Tokyo, UTC+9 — always offset from UTC)
                    Calendar calTokyo = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
                    Timestamp tsTokyo = rs.getTimestamp("test_timestamp", calTokyo);
                    assertThat(tsTokyo).isNotNull();

                    // Calendar changes how the literal is interpreted: Tokyo interprets 12:00 as JST.
                    // 2024-03-15 12:00:00 JST = 2024-03-15 03:00:00 UTC → verify explicit instant.
                    assertThat(tsTokyo.toInstant()).isEqualTo(Instant.parse("2024-03-15T03:00:00Z"));

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testMultipleSessionTimezones() {
        // Naive TIMESTAMP preserves the literal wall-clock regardless of session timezone.
        // All session timezones must return LocalDateTime with hour=12 for a 12:00:00 literal.
        String sql = "SELECT TIMESTAMP '2024-06-15 12:00:00' as test_timestamp";

        for (String sessionTz : new String[] {"UTC", "America/New_York", "Asia/Tokyo", "Europe/London"}) {
            Properties props = new Properties();
            props.setProperty("querySetting.time_zone", sessionTz);
            try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Timestamp is non-null
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).as("session=%s", sessionTz).isNotNull();

                    // LocalDateTime always returns the raw literal (hour=12 regardless of session TZ)
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    assertThat(ldt.getHour())
                            .as("literal hour preserved for session=%s", sessionTz)
                            .isEqualTo(12);
                    assertThat(ldt.getMinute()).isEqualTo(0);
                    assertThat(ldt.getSecond()).isEqualTo(0);
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testTimestampPrecision() {
        // Test that nanosecond precision is preserved
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Use a timestamp with microsecond precision (Hyper's max)
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test Instant preserves precision
                    Instant instant = rs.getObject("test_timestamp", Instant.class);
                    assertThat(instant).isNotNull();
                    long nanos = instant.getNano();
                    // Verify we have microsecond precision (123456000 nanos)
                    assertThat(nanos).isEqualTo(123456000);

                    // Test OffsetDateTime preserves precision
                    OffsetDateTime odt = rs.getObject("test_timestamp", OffsetDateTime.class);
                    assertThat(odt).isNotNull();
                    assertThat(odt.getNano()).isEqualTo(123456000);

                    // Test Timestamp preserves precision
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).isNotNull();
                    assertThat(ts.getNanos()).isGreaterThan(0);

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testInvalidTimezoneHandling() {
        // Test that invalid timezone in query settings is rejected by the driver before reaching Hyper
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "Invalid/Timezone");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' as test_timestamp";

                assertThatThrownBy(() -> stmt.executeQuery(sql))
                        .isInstanceOf(java.sql.SQLException.class)
                        .hasMessageContaining("Invalid timezone setting 'Invalid/Timezone'");
            }
        }
    }

    @Test
    @SneakyThrows
    public void testTimestampWithTimezoneColumn() {
        // Test TIMESTAMPTZ (timestamp with timezone) - getString() must include a UTC offset suffix
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Use AT TIME ZONE to convert to TIMESTAMPTZ
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' AT TIME ZONE 'UTC' as test_timestamptz";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // getString() for TIMESTAMPTZ must include a UTC offset
                    String tsString = rs.getString("test_timestamptz");
                    assertThat(tsString).isNotNull();
                    assertThat(tsString)
                            .matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6} [+-]\\d{2}:\\d{2}");

                    // Should be able to get as Instant regardless of timezone metadata
                    Instant instant = rs.getObject("test_timestamptz", Instant.class);
                    assertThat(instant).isNotNull();
                    // TIMESTAMP '2024-03-15 10:00:00' AT TIME ZONE 'UTC' is 10:00 UTC
                    assertThat(instant).isEqualTo(Instant.parse("2024-03-15T10:00:00Z"));

                    // Should be able to get as OffsetDateTime
                    OffsetDateTime odt = rs.getObject("test_timestamptz", OffsetDateTime.class);
                    assertThat(odt).isNotNull();
                    assertThat(odt.toInstant()).isEqualTo(instant);

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testTimestampNullHandling() {
        // Test that NULL timestamps are handled correctly
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT NULL::TIMESTAMP as null_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // All methods should return null
                    assertThat(rs.getTimestamp("null_timestamp")).isNull();
                    assertThat(rs.wasNull()).isTrue();

                    assertThat(rs.getString("null_timestamp")).isNull();
                    assertThat(rs.wasNull()).isTrue();

                    assertThat(rs.getObject("null_timestamp", Instant.class)).isNull();
                    assertThat(rs.wasNull()).isTrue();

                    assertThat(rs.getObject("null_timestamp", OffsetDateTime.class))
                            .isNull();
                    assertThat(rs.wasNull()).isTrue();

                    assertThat(rs.getObject("null_timestamp", LocalDateTime.class))
                            .isNull();
                    assertThat(rs.wasNull()).isTrue();

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testDefaultSessionTimezone() {
        // Test without setting any timezone - getTimestamp() must use the JVM system default.
        // We prove this by comparing getTimestamp() (no calendar) against
        // getTimestamp(Calendar.getInstance(TimeZone.getDefault())): both must encode the literal
        // in the same timezone, so their epoch millis must be identical.
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    Timestamp tsNoCalendar = rs.getTimestamp("test_timestamp");
                    assertThat(tsNoCalendar).isNotNull();

                    Calendar sysDefaultCal = Calendar.getInstance(TimeZone.getDefault());
                    Timestamp tsWithSysDefault = rs.getTimestamp("test_timestamp", sysDefaultCal);

                    // getTimestamp() with no calendar must use the system default timezone —
                    // the epoch millis must equal getTimestamp(Calendar using TimeZone.getDefault())
                    assertThat(tsNoCalendar.getTime()).isEqualTo(tsWithSysDefault.getTime());

                    // toString() renders in the JVM default timezone, so the literal digits are
                    // preserved: if the encoding is correct, we see "10:00:00" regardless of which
                    // timezone the system is in.
                    assertThat(tsNoCalendar.toString()).startsWith("2024-03-15 10:00:00");

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetDateAndTimeFromTimestamp() {
        // Test getDate() and getTime() methods on timestamp columns
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test getDate - should return java.sql.Date
                    java.sql.Date date = rs.getDate("test_timestamp");
                    assertThat(date).isNotNull();

                    // Test getTime - should return java.sql.Time
                    java.sql.Time time = rs.getTime("test_timestamp");
                    assertThat(time).isNotNull();

                    // Test getTimestamp - should return java.sql.Timestamp
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).isNotNull();

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetObjectWithoutTypeParameter() {
        // Test getObject() without type parameter - should return Timestamp by default
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test getObject without type parameter
                    Object obj = rs.getObject("test_timestamp");
                    assertThat(obj).isNotNull();
                    assertThat(obj).isInstanceOf(Timestamp.class);

                    // Test that returned object has correct value
                    Timestamp ts = (Timestamp) obj;
                    assertThat(ts.getNanos()).isGreaterThan(0);

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetObjectWithStringType() {
        // Test getObject(String.class) to ensure string conversion works
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test getObject with String type
                    String str = rs.getObject("test_timestamp", String.class);
                    assertThat(str).isEqualTo("2024-03-15 14:30:45.123456");

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetObjectWithDateAndTimeTypes() {
        // Test getObject with Date and Time class types
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test getObject with Date type
                    java.sql.Date date = rs.getObject("test_timestamp", java.sql.Date.class);
                    assertThat(date).isNotNull();

                    // Test getObject with Time type
                    java.sql.Time time = rs.getObject("test_timestamp", java.sql.Time.class);
                    assertThat(time).isNotNull();

                    // Test getObject with Timestamp type
                    Timestamp ts = rs.getObject("test_timestamp", Timestamp.class);
                    assertThat(ts).isNotNull();

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetObjectWithNullTypeThrows() {
        // Test getObject with null type parameter - should throw SQLException
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    assertThatThrownBy(() -> rs.getObject("test_timestamp", (Class<?>) null))
                            .isInstanceOf(java.sql.SQLException.class)
                            .hasMessageContaining("must not be null");
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testNaiveTimestampPreservesLiteralValue() {
        // Regression test: Verify that naive TIMESTAMP preserves literal value
        // Bug in 0.42.1: TIMESTAMP '2024-03-15 10:00:00' with LA timezone returned '03:00:00' (7hr offset)
        // Expected: Should return '10:00:00' (preserves literal)
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "America/Los_Angeles"); // -07:00 offset

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // CRITICAL: Naive timestamp should preserve literal value, not apply timezone offset
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).isNotNull();
                    assertThat(ts.toString()).startsWith("2024-03-15 10:00:00");

                    // String representation should also match literal value
                    String str = rs.getString("test_timestamp");
                    assertThat(str).startsWith("2024-03-15 10:00:00");

                    // LocalDateTime should match literal components
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    assertThat(ldt.getYear()).isEqualTo(2024);
                    assertThat(ldt.getMonthValue()).isEqualTo(3);
                    assertThat(ldt.getDayOfMonth()).isEqualTo(15);
                    assertThat(ldt.getHour()).isEqualTo(10); // NOT 3! Should preserve literal
                    assertThat(ldt.getMinute()).isEqualTo(0);
                    assertThat(ldt.getSecond()).isEqualTo(0);

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetObjectEqualsGetTimestamp() {
        // getObject() and getTimestamp() must return equivalent values for both TIMESTAMP and TIMESTAMPTZ
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Naive TIMESTAMP
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as ts,"
                        + " TIMESTAMP '2024-03-15 14:30:45.123456' AT TIME ZONE 'UTC' as tstz";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    Timestamp tsFromGetObject = (Timestamp) rs.getObject("ts");
                    Timestamp tsFromGetTimestamp = rs.getTimestamp("ts");
                    assertThat(tsFromGetObject.getTime()).isEqualTo(tsFromGetTimestamp.getTime());
                    assertThat(tsFromGetObject.getNanos()).isEqualTo(tsFromGetTimestamp.getNanos());
                    assertThat(tsFromGetObject.toString()).isEqualTo(tsFromGetTimestamp.toString());

                    Timestamp tstzFromGetObject = (Timestamp) rs.getObject("tstz");
                    Timestamp tstzFromGetTimestamp = rs.getTimestamp("tstz");
                    assertThat(tstzFromGetObject.getTime()).isEqualTo(tstzFromGetTimestamp.getTime());
                    assertThat(tstzFromGetObject.getNanos()).isEqualTo(tstzFromGetTimestamp.getNanos());
                    assertThat(tstzFromGetObject.toString()).isEqualTo(tstzFromGetTimestamp.toString());
                }
            }
        }
    }

    // ========== Roundtrip parameterized query tests ==========

    /**
     * Provides test cases for the roundtrip parameterized timestamp test.
     *
     * Each case specifies:
     * - castType: SQL cast to apply ("timestamp" or "timestamptz")
     * - sessionTimezone: timezone set via querySetting.time_zone
     * - writeCalendarTz: timezone for Calendar passed to setTimestamp (null = no calendar)
     * - readCalendarTz: timezone for Calendar passed to getTimestamp (null = no calendar)
     *
     * Parameters are serialized as Arrow TimeStampMicroTZVector(UTC). The write path uses the
     * Timestamp's actual UTC instant ({@code Timestamp.toInstant()}) directly. The Calendar
     * parameter on {@code setTimestamp} does not affect the stored instant, because we always
     * serialize as UTC epoch microseconds.
     *
     * On the Hyper side, when casting the TIMESTAMPTZ parameter:
     * - {@code ?::timestamptz}: the value stays as TIMESTAMPTZ (no conversion). The roundtrip
     *   always preserves the original instant.
     * - {@code ?::timestamp}: Hyper converts the UTC instant to the session timezone, then
     *   strips the timezone. The resulting naive TIMESTAMP depends on the session timezone,
     *   so roundtrip preservation is only guaranteed when session timezone matches the JVM
     *   default timezone.
     *
     * On the read side:
     * - TIMESTAMPTZ: the TZ accessor returns the true UTC instant
     * - TIMESTAMP (naive): the accessor interprets the literal value in the JVM's default
     *   timezone
     *
     * This test exercises all combinations to ensure no errors occur, java.time types are
     * internally consistent, and TIMESTAMPTZ roundtrips always preserve the instant.
     */
    static Stream<Arguments> timestampRoundtripCases() {
        List<Arguments> cases = new ArrayList<>();
        String[] castTypes = {"timestamp", "timestamptz"};
        String[] sessionTimezones = {"UTC", "America/Los_Angeles"};
        // null means no calendar
        String[] calendarTimezones = {null, "UTC", "America/Los_Angeles", "Asia/Tokyo"};

        for (String castType : castTypes) {
            for (String sessionTz : sessionTimezones) {
                for (String writeTz : calendarTimezones) {
                    for (String readTz : calendarTimezones) {
                        cases.add(Arguments.of(castType, sessionTz, writeTz, readTz));
                    }
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "cast={0} session={1} writeCal={2} readCal={3}")
    @MethodSource("timestampRoundtripCases")
    @SneakyThrows
    public void testTimestampRoundtrip(String castType, String sessionTz, String writeCalTz, String readCalTz) {
        val input = Timestamp.valueOf(LocalDateTime.of(2024, 6, 15, 14, 30, 45));

        Calendar writeCal = writeCalTz != null ? Calendar.getInstance(TimeZone.getTimeZone(writeCalTz)) : null;
        Calendar readCal = readCalTz != null ? Calendar.getInstance(TimeZone.getTimeZone(readCalTz)) : null;

        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", sessionTz);

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            // Cast the parameter to the target type so Hyper returns it as TIMESTAMP or TIMESTAMPTZ
            String sql = "SELECT (?::" + castType + ") AS ts";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                if (writeCal != null) {
                    pstmt.setTimestamp(1, input, writeCal);
                } else {
                    pstmt.setTimestamp(1, input);
                }

                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).as("result set should have a row").isTrue();

                    // Read back with the specified calendar
                    Timestamp result;
                    if (readCal != null) {
                        result = rs.getTimestamp("ts", readCal);
                    } else {
                        result = rs.getTimestamp("ts");
                    }
                    assertThat(result)
                            .as("returned timestamp should not be null")
                            .isNotNull();

                    // Verify java.time types are also retrievable
                    Instant instant = rs.getObject("ts", Instant.class);
                    assertThat(instant).as("Instant should be non-null").isNotNull();

                    LocalDateTime ldt = rs.getObject("ts", LocalDateTime.class);
                    assertThat(ldt).as("LocalDateTime should be non-null").isNotNull();

                    OffsetDateTime odt = rs.getObject("ts", OffsetDateTime.class);
                    assertThat(odt).as("OffsetDateTime should be non-null").isNotNull();

                    ZonedDateTime zdt = rs.getObject("ts", ZonedDateTime.class);
                    assertThat(zdt).as("ZonedDateTime should be non-null").isNotNull();

                    // All java.time types should represent the same instant
                    assertThat(odt.toInstant()).isEqualTo(instant);
                    assertThat(zdt.toInstant()).isEqualTo(instant);

                    // For TIMESTAMPTZ, the stored instant must match the input's instant.
                    // We compare via Instant (not Timestamp.getTime) because getTimestamp(Calendar)
                    // shifts the wall-clock representation, changing the epoch millis.
                    // Example: getTimestamp(tokyoCal) on a value whose UTC instant is 14:30 returns
                    // a Timestamp whose getTime() encodes "14:30 JST" (a different UTC epoch), so
                    // epoch-millis comparison would fail even for a correct roundtrip.
                    if ("timestamptz".equals(castType)) {
                        assertThat(instant)
                                .as("TIMESTAMPTZ roundtrip should preserve the instant")
                                .isEqualTo(input.toInstant());
                    }

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }
}
