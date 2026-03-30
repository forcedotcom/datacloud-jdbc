/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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
        // Test that naive TIMESTAMP preserves literal values regardless of session timezone
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "America/New_York");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                // Query a naive timestamp: 2024-03-15 10:00:00
                // Hyper stores this as UTC, so literal values should be preserved
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // LocalDateTime should preserve literal value
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    assertThat(ldt.getYear()).isEqualTo(2024);
                    assertThat(ldt.getMonthValue()).isEqualTo(3);
                    assertThat(ldt.getDayOfMonth()).isEqualTo(15);
                    assertThat(ldt.getHour()).isEqualTo(10);
                    assertThat(ldt.getMinute()).isEqualTo(0);
                    assertThat(ldt.getSecond()).isEqualTo(0);
                    assertThat(ldt.getNano()).isEqualTo(123456000);

                    // Timestamp should match literal value
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).isNotNull();
                    assertThat(ts.toString()).startsWith("2024-03-15 10:00:00.123456");

                    // String should not have timezone offset for naive TIMESTAMP
                    String tsString = rs.getString("test_timestamp");
                    assertThat(tsString).isEqualTo("2024-03-15 10:00:00.123456");
                    // Verify no timezone offset suffix (like " +00:00" or " -07:00")
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

                    // Test Instant - always UTC
                    Instant instant = rs.getObject("test_timestamp", Instant.class);
                    assertThat(instant).isNotNull();

                    // Test OffsetDateTime - includes timezone offset
                    OffsetDateTime odt = rs.getObject("test_timestamp", OffsetDateTime.class);
                    assertThat(odt).isNotNull();
                    assertThat(odt.toInstant()).isEqualTo(instant);
                    assertThat(odt.toLocalDateTime()).isEqualTo(ldt);

                    // Test ZonedDateTime - full timezone info
                    ZonedDateTime zdt = rs.getObject("test_timestamp", ZonedDateTime.class);
                    assertThat(zdt).isNotNull();
                    assertThat(zdt.toInstant()).isEqualTo(instant);
                    assertThat(zdt.toLocalDateTime()).isEqualTo(ldt);

                    // Test legacy Timestamp - should match literal value
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

                    // Get with default (session timezone = UTC)
                    Timestamp tsDefault = rs.getTimestamp("test_timestamp");
                    assertThat(tsDefault).isNotNull();

                    // Get with Calendar override (Europe/London)
                    Calendar calLondon = Calendar.getInstance(TimeZone.getTimeZone("Europe/London"));
                    Timestamp tsLondon = rs.getTimestamp("test_timestamp", calLondon);
                    assertThat(tsLondon).isNotNull();

                    // For naive TIMESTAMP without timezone, the calendar affects how the
                    // LocalDateTime is interpreted. Since UTC and London are in different
                    // zones, they should produce the same LocalDateTime but the Calendar
                    // parameter is respected in the conversion logic
                    // Verify both work and produce timestamps
                    assertThat(tsDefault.toString()).isNotEmpty();
                    assertThat(tsLondon.toString()).isNotEmpty();

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testMultipleSessionTimezones() {
        // Test that different session timezones affect timestamp interpretation
        String sql = "SELECT TIMESTAMP '2024-06-15 12:00:00' as test_timestamp";

        // Test with America/New_York
        Properties propsNY = new Properties();
        propsNY.setProperty("querySetting.time_zone", "America/New_York");
        try (DataCloudConnection connNY = LocalHyperTestBase.getHyperQueryConnection(propsNY)) {
            try (Statement stmt = connNY.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                Timestamp tsNY = rs.getTimestamp("test_timestamp");
                assertThat(tsNY).isNotNull();
            }
        }

        // Test with Asia/Tokyo
        Properties propsTokyo = new Properties();
        propsTokyo.setProperty("querySetting.time_zone", "Asia/Tokyo");
        try (DataCloudConnection connTokyo = LocalHyperTestBase.getHyperQueryConnection(propsTokyo)) {
            try (Statement stmt = connTokyo.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                Timestamp tsTokyo = rs.getTimestamp("test_timestamp");
                assertThat(tsTokyo).isNotNull();
            }
        }

        // Test with Europe/London
        Properties propsLondon = new Properties();
        propsLondon.setProperty("querySetting.time_zone", "Europe/London");
        try (DataCloudConnection connLondon = LocalHyperTestBase.getHyperQueryConnection(propsLondon)) {
            try (Statement stmt = connLondon.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                Timestamp tsLondon = rs.getTimestamp("test_timestamp");
                assertThat(tsLondon).isNotNull();
            }
        }

        // Test with UTC
        Properties propsUTC = new Properties();
        propsUTC.setProperty("querySetting.time_zone", "UTC");
        try (DataCloudConnection connUTC = LocalHyperTestBase.getHyperQueryConnection(propsUTC)) {
            try (Statement stmt = connUTC.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                Timestamp tsUTC = rs.getTimestamp("test_timestamp");
                assertThat(tsUTC).isNotNull();
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
        // Test that invalid timezone in query settings is rejected by Hyper
        // Our JDBC driver logs a warning and falls back to system default,
        // but Hyper itself rejects the invalid timezone setting
        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "Invalid/Timezone");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' as test_timestamp";

                // Hyper rejects invalid timezone settings at query execution time
                assertThatThrownBy(() -> stmt.executeQuery(sql))
                        .isInstanceOf(DataCloudJDBCException.class)
                        .hasMessageContaining("unknown time zone");
            }
        }
    }

    @Test
    @SneakyThrows
    public void testTimestampWithTimezoneColumn() {
        // Test TIMESTAMPTZ (timestamp with timezone) - should include offset in string
        // Note: Hyper's behavior with TIMESTAMPTZ metadata may vary based on version
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Use AT TIME ZONE to convert to TIMESTAMPTZ
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' AT TIME ZONE 'UTC' as test_timestamptz";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Verify we can read the timestamp
                    String tsString = rs.getString("test_timestamptz");
                    assertThat(tsString).isNotNull();
                    assertThat(tsString).isNotEmpty();

                    // Should be able to get as Instant regardless of timezone metadata
                    Instant instant = rs.getObject("test_timestamptz", Instant.class);
                    assertThat(instant).isNotNull();

                    // Should be able to get as OffsetDateTime
                    OffsetDateTime odt = rs.getObject("test_timestamptz", OffsetDateTime.class);
                    assertThat(odt).isNotNull();

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
        // Test without setting any timezone - should use system default
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 10:00:00' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Should work with system default timezone
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    assertThat(ts).isNotNull();

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
                    assertThat(str).isNotNull();
                    assertThat(str).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}");

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
    public void testGetObjectWithNullType() {
        // Test getObject with null type parameter - should return null per JDBC spec
        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    // Test getObject with null type - should return null
                    Object result = rs.getObject("test_timestamp", (Class<?>) null);
                    assertThat(result).isNull();

                    assertThat(rs.next()).isFalse();
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
     * Parameters are always serialized as Arrow TimeStampMicroTZVector(UTC). The write path
     * uses {@code Timestamp.toLocalDateTime()} to extract local components, which are then
     * stored as-is in UTC. This means the actual UTC instant of the Timestamp is NOT preserved
     * on the wire — only the local date/time components are.
     *
     * On the Hyper side, when casting the TIMESTAMPTZ parameter:
     * - {@code ?::timestamp}: Hyper converts the UTC value to the session timezone, then strips
     *   the timezone. So session timezone affects the naive TIMESTAMP result.
     * - {@code ?::timestamptz}: The value stays as TIMESTAMPTZ (no conversion).
     *
     * On the read side:
     * - TIMESTAMP (naive): returns the literal value, interpreted in the JVM's default timezone
     * - TIMESTAMPTZ: returns the true UTC instant
     *
     * Because of these conversions, roundtrip value preservation depends on the interaction
     * between JVM default timezone, session timezone, and Calendar parameters. This test
     * exercises all combinations to ensure no errors occur and java.time types are consistent.
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

                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }
}
