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
import java.sql.Types;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for timezone and timestamp handling in DataCloud JDBC driver.
 *
 * <p>Tests the timezone precedence order:
 * 1. Calendar parameter (per-call setting)
 * 2. Arrow metadata timezone (from Hyper - TIMESTAMPTZ columns)
 * 3. Session timezone (query setting time_zone)
 * 4. System default
 *
 * <p>Also contains the PreparedStatement parameter-setting matrix: every combination of setter
 * method, Java type, and SQL cast target ({@code ?::timestamp} / {@code ?::timestamptz}).
 */
@ExtendWith(LocalHyperTestBase.class)
public class TimeZoneIntegrationTest {

    // ── Parameter matrix constants ────────────────────────────────────────────────────────────
    // Input: UTC instant 2024-06-15T21:30:45.123456Z
    // In JVM TZ (LA, UTC-7 in June): wall-clock = "2024-06-15 14:30:45.123456"
    private static final Instant MATRIX_INPUT_INSTANT = Instant.parse("2024-06-15T21:30:45.123456Z");
    private static final LocalDateTime MATRIX_INPUT_LDT =
            LocalDateTime.ofInstant(MATRIX_INPUT_INSTANT, ZoneId.of("America/Los_Angeles")); // 14:30:45
    private static final OffsetDateTime MATRIX_INPUT_ODT =
            OffsetDateTime.ofInstant(MATRIX_INPUT_INSTANT, ZoneOffset.UTC);
    private static final ZonedDateTime MATRIX_INPUT_ZDT = ZonedDateTime.ofInstant(MATRIX_INPUT_INSTANT, ZoneOffset.UTC);
    private static final Timestamp MATRIX_INPUT_TS = Timestamp.from(MATRIX_INPUT_INSTANT);

    /** 14:30:45 — JVM (LA) wall-clock digits, stored as a naive literal. */
    private static final LocalDateTime MATRIX_WALL_CLOCK_LA =
            LocalDateTime.ofInstant(MATRIX_INPUT_INSTANT, ZoneId.of("America/Los_Angeles"));

    /** 21:30:45 — UTC wall-clock digits (= the instant read in UTC). */
    private static final LocalDateTime MATRIX_WALL_CLOCK_UTC =
            LocalDateTime.ofInstant(MATRIX_INPUT_INSTANT, ZoneOffset.UTC);

    /** 21:30:45Z — the original UTC instant, preserved exactly. */
    private static final OffsetDateTime MATRIX_INSTANT_UTC =
            OffsetDateTime.ofInstant(MATRIX_INPUT_INSTANT, ZoneOffset.UTC);

    /**
     * 14:30:45Z — wall-clock literal (14:30) encoded as naive UTC epoch, then cast to timestamptz
     * with session TZ=UTC. Diverges from PG which sends the true UTC epoch (21:30:45Z).
     */
    private static final OffsetDateTime MATRIX_WALL_CLOCK_AS_UTC =
            OffsetDateTime.of(MATRIX_WALL_CLOCK_LA, ZoneOffset.UTC);

    private static TimeZone matrixOriginalTimeZone;

    @BeforeAll
    static void pinJvmTimezoneForMatrix() {
        matrixOriginalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    }

    @AfterAll
    static void restoreJvmTimezoneAfterMatrix() {
        TimeZone.setDefault(matrixOriginalTimeZone);
    }

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
     * Parameters are serialized as Arrow TimeStampMicroVector (naive, no timezone metadata).
     * The write path extracts the wall-clock digits of the Timestamp in the effective write
     * timezone (writeCalendarTz if given, else JVM default) and sends those digits as the
     * naive literal. This matches JDBC spec semantics for {@code setTimestamp(Calendar)}.
     *
     * On the Hyper side:
     * - {@code ?::timestamp}: Hyper stores the received naive literal directly. The wall-clock
     *   is preserved regardless of session timezone.
     * - {@code ?::timestamptz}: Hyper interprets the naive literal in the session timezone to
     *   produce an instant. The stored instant equals the wall-clock literal interpreted in
     *   the session timezone.
     *
     * On the read side:
     * - TIMESTAMP (naive): the accessor interprets the literal in the JVM's default timezone
     *   (or the read Calendar's timezone if provided).
     * - TIMESTAMPTZ: the TZ accessor returns the stored UTC instant.
     *
     * This test exercises all combinations to ensure no errors occur and java.time types are
     * internally consistent. For TIMESTAMP, the wall-clock written equals the wall-clock read.
     * For TIMESTAMPTZ, the stored instant equals the wall-clock in the write timezone
     * interpreted in the session timezone.
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

                    // Effective write timezone: Calendar TZ if provided, else JVM default.
                    ZoneId writeTz = writeCalTz != null ? ZoneId.of(writeCalTz) : ZoneId.systemDefault();

                    // The wall-clock digits written to Hyper: the input instant projected into
                    // the write timezone.
                    LocalDateTime writtenWallClock = LocalDateTime.ofInstant(input.toInstant(), writeTz);

                    if ("timestamp".equals(castType)) {
                        // TIMESTAMP: naive literal is preserved end-to-end. The literal stored
                        // equals the wall-clock the caller expressed in the write timezone.
                        assertThat(ldt)
                                .as("TIMESTAMP wall-clock should equal the written literal")
                                .isEqualTo(writtenWallClock);
                    } else {
                        // TIMESTAMPTZ: Hyper interprets the naive literal in the session timezone
                        // to produce the stored instant. So the stored instant equals the
                        // written wall-clock re-interpreted in the session timezone.
                        // Example: written literal "14:30" with session=UTC → instant 14:30Z.
                        //          written literal "14:30" with session=LA  → instant 21:30Z.
                        Instant expectedInstant =
                                writtenWallClock.atZone(ZoneId.of(sessionTz)).toInstant();
                        assertThat(instant)
                                .as("TIMESTAMPTZ instant should be wall-clock interpreted in session TZ")
                                .isEqualTo(expectedInstant);
                    }

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    // ── PreparedStatement parameter-setting matrix ────────────────────────────────────────────

    @FunctionalInterface
    interface ParameterSetter {
        void set(PreparedStatement pstmt) throws Exception;
    }

    /**
     * Matrix of all supported setter/type/SQL-cast combinations.
     *
     * <p>Each row: (description, setter, castType, expectedLDT, expectedODT)
     * <ul>
     *   <li>{@code expectedLDT} non-null → {@code ?::timestamp}: asserts
     *       {@code getObject(LocalDateTime.class)}</li>
     *   <li>{@code expectedODT} non-null → {@code ?::timestamptz}: asserts
     *       {@code getObject(OffsetDateTime.class)}</li>
     * </ul>
     *
     * <p>JVM TZ is pinned to {@code America/Los_Angeles} (UTC-7 in June) so that
     * timezone-sensitive differences are visible. Session TZ is UTC for all queries.
     *
     * <h2>JDBC 4.2 Spec mapping (Table B-4)</h2>
     * <pre>
     * java.sql.Timestamp           → TIMESTAMP  (wall-clock in JVM TZ)
     * java.time.Instant            → TIMESTAMP_WITH_TIMEZONE (UTC epoch)
     * java.time.LocalDateTime      → TIMESTAMP  (LDT digits stored as-is)
     * java.time.OffsetDateTime     → TIMESTAMP_WITH_TIMEZONE (UTC epoch)
     * java.time.ZonedDateTime      → TIMESTAMP_WITH_TIMEZONE (UTC epoch)
     * </pre>
     */
    static Stream<Arguments> parameterMatrixCases() {
        return Stream.of(
                // ── setTimestamp (no calendar) ─────────────────────────────────────────────
                // Wall-clock normalization: JVM TZ (LA) wall-clock = 14:30:45 stored as naive literal.
                Arguments.of(
                        "setTimestamp(ts) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setTimestamp(1, MATRIX_INPUT_TS),
                        "timestamp",
                        MATRIX_WALL_CLOCK_LA,
                        null),
                // ?::timestamptz: Hyper interprets naive literal 14:30:45 in session UTC → 14:30:45Z.
                // (PG JDBC diverges: sends true UTC epoch 21:30:45Z instead.)
                Arguments.of(
                        "setTimestamp(ts) → ?::timestamptz",
                        (ParameterSetter) pstmt -> pstmt.setTimestamp(1, MATRIX_INPUT_TS),
                        "timestamptz",
                        null,
                        MATRIX_WALL_CLOCK_AS_UTC),

                // ── setTimestamp with Calendar UTC ─────────────────────────────────────────
                // Calendar UTC wall-clock = 21:30:45. Stored as naive literal 21:30:45.
                Arguments.of(
                        "setTimestamp(ts, calUTC) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setTimestamp(
                                1, MATRIX_INPUT_TS, Calendar.getInstance(TimeZone.getTimeZone("UTC"))),
                        "timestamp",
                        MATRIX_WALL_CLOCK_UTC,
                        null),

                // ── setObject(Timestamp) ───────────────────────────────────────────────────
                // Delegates to setTimestamp(ts). JVM wall-clock 14:30:45.
                Arguments.of(
                        "setObject(Timestamp) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_TS),
                        "timestamp",
                        MATRIX_WALL_CLOCK_LA,
                        null),

                // ── setObject(Instant) ────────────────────────────────────────────────────
                // JDBC 4.2: Instant → TIMESTAMP_WITH_TIMEZONE. UTC epoch stored exactly.
                Arguments.of(
                        "setObject(Instant) → ?::timestamptz",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_INSTANT),
                        "timestamptz",
                        null,
                        MATRIX_INSTANT_UTC),
                Arguments.of(
                        "setObject(Instant) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_INSTANT),
                        "timestamp",
                        MATRIX_WALL_CLOCK_UTC,
                        null),

                // ── setObject(LocalDateTime) ──────────────────────────────────────────────
                // JDBC 4.2: LocalDateTime → TIMESTAMP. LDT digits stored as-is (no TZ shift).
                // Recommended write path for wall-clock (TIMESTAMP without timezone) values.
                Arguments.of(
                        "setObject(LocalDateTime) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_LDT),
                        "timestamp",
                        MATRIX_WALL_CLOCK_LA,
                        null),

                // ── setObject(OffsetDateTime) ─────────────────────────────────────────────
                // JDBC 4.2: OffsetDateTime → TIMESTAMP_WITH_TIMEZONE. UTC epoch stored.
                // Recommended write path for exact-instant (TIMESTAMPTZ) values.
                Arguments.of(
                        "setObject(OffsetDateTime) → ?::timestamptz",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_ODT),
                        "timestamptz",
                        null,
                        MATRIX_INSTANT_UTC),
                Arguments.of(
                        "setObject(OffsetDateTime) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_ODT),
                        "timestamp",
                        MATRIX_WALL_CLOCK_UTC,
                        null),

                // ── setObject(ZonedDateTime) ──────────────────────────────────────────────
                // JDBC 4.2: ZonedDateTime → TIMESTAMP_WITH_TIMEZONE. UTC epoch stored.
                Arguments.of(
                        "setObject(ZonedDateTime) → ?::timestamptz",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_ZDT),
                        "timestamptz",
                        null,
                        MATRIX_INSTANT_UTC),

                // ── setObject(ts, Types.TIMESTAMP) ────────────────────────────────────────
                // Explicit TIMESTAMP type hint. JVM wall-clock 14:30:45.
                Arguments.of(
                        "setObject(ts, TIMESTAMP) → ?::timestamp",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_TS, Types.TIMESTAMP),
                        "timestamp",
                        MATRIX_WALL_CLOCK_LA,
                        null),

                // ── setObject(ts, Types.TIMESTAMP_WITH_TIMEZONE) ──────────────────────────
                // Explicit TIMESTAMPTZ type hint. UTC epoch from Timestamp.toInstant() stored.
                // Use this when you have a java.sql.Timestamp but need TIMESTAMPTZ roundtrip.
                Arguments.of(
                        "setObject(ts, TIMESTAMP_WITH_TIMEZONE) → ?::timestamptz",
                        (ParameterSetter) pstmt -> pstmt.setObject(1, MATRIX_INPUT_TS, Types.TIMESTAMP_WITH_TIMEZONE),
                        "timestamptz",
                        null,
                        MATRIX_INSTANT_UTC));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameterMatrixCases")
    @SneakyThrows
    void verifyParameterMatrix(
            String description,
            ParameterSetter setter,
            String castType,
            LocalDateTime expectedLDT,
            OffsetDateTime expectedODT) {

        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "UTC");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            String sql = "SELECT (?::" + castType + ") AS val";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                setter.set(pstmt);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();

                    if (expectedLDT != null) {
                        assertThat(rs.getObject("val", LocalDateTime.class))
                                .as("%s — getObject(LocalDateTime.class)", description)
                                .isEqualTo(expectedLDT);
                    }

                    if (expectedODT != null) {
                        assertThat(rs.getObject("val", OffsetDateTime.class))
                                .as("%s — getObject(OffsetDateTime.class)", description)
                                .isEqualTo(expectedODT);
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    void nullTimestampParameterStoresSqlNull() {
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(new Properties())) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT (?::timestamp) AS val")) {
                pstmt.setNull(1, Types.TIMESTAMP);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("val")).isNull();
                    assertThat(rs.wasNull()).isTrue();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    void nullTimestampTZParameterStoresSqlNull() {
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(new Properties())) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT (?::timestamptz) AS val")) {
                pstmt.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getObject("val")).isNull();
                    assertThat(rs.wasNull()).isTrue();
                }
            }
        }
    }
}
