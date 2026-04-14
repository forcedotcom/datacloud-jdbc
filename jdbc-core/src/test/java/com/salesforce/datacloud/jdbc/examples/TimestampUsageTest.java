/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Examples demonstrating how to correctly read and write timestamp values using the
 * DataCloud JDBC driver.
 *
 * <h2>Rule of thumb</h2>
 * <ul>
 *   <li>If the value has a timezone ({@code TIMESTAMPTZ}), use {@link OffsetDateTime}.</li>
 *   <li>If it doesn't ({@code TIMESTAMP}), use {@link LocalDateTime}.</li>
 * </ul>
 *
 * <h2>Use-case summary</h2>
 * <pre>
 * Use case              | Write                       | Read
 * ──────────────────────┼─────────────────────────────┼──────────────────────────────
 * Wall-clock (TIMESTAMP)| setObject(LocalDateTime)    | getObject(LocalDateTime.class)
 * UTC instant (TIMESTAMPTZ)| setObject(OffsetDateTime)| getObject(OffsetDateTime.class)
 * Legacy (TIMESTAMP)    | setTimestamp(ts)            | getTimestamp()
 * </pre>
 */
@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class TimestampUsageTest {

    /**
     * Wall-clock TIMESTAMP: store and retrieve a date-time with no timezone context.
     *
     * <p>Use {@link LocalDateTime} when the value represents a calendar moment that should be
     * stored as literal digits — e.g., "meeting at 2pm". The JVM timezone does not affect
     * the stored value.
     */
    @Test
    @SneakyThrows
    public void wallClockTimestamp() {
        LocalDateTime meetingTime = LocalDateTime.of(2024, 6, 15, 14, 30, 0);

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT (?::timestamp) AS ts")) {
                // Write: digits stored as-is, no timezone shift applied
                pstmt.setObject(1, meetingTime);

                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();

                    // Read: get back the exact same wall-clock digits
                    LocalDateTime result = rs.getObject("ts", LocalDateTime.class);
                    assertThat(result).isEqualTo(meetingTime);

                    log.info("Wall-clock TIMESTAMP: written={}, read={}", meetingTime, result);
                }
            }
        }
    }

    /**
     * UTC instant TIMESTAMPTZ: store and retrieve an exact moment in time.
     *
     * <p>Use {@link OffsetDateTime} when the value represents a specific instant — e.g.,
     * "event occurred at 21:30:45 UTC". The UTC epoch is preserved exactly across write and read.
     */
    @Test
    @SneakyThrows
    public void utcInstantTimestamptz() {
        OffsetDateTime eventTime = OffsetDateTime.of(2024, 6, 15, 21, 30, 45, 123_456_000, ZoneOffset.UTC);

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT (?::timestamptz) AS ts")) {
                // Write: UTC epoch preserved exactly
                pstmt.setObject(1, eventTime);

                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();

                    // Read: get back the same UTC instant
                    OffsetDateTime result = rs.getObject("ts", OffsetDateTime.class);
                    assertThat(result).isEqualTo(eventTime);

                    log.info("UTC instant TIMESTAMPTZ: written={}, read={}", eventTime, result);
                }
            }
        }
    }

    /**
     * Legacy java.sql.Timestamp: for compatibility with existing JDBC code.
     *
     * <p>{@code setTimestamp} stores the wall-clock value in the JVM default timezone as a naive
     * {@code TIMESTAMP}. Use this only for {@code TIMESTAMP} (no timezone) columns where you
     * already have a {@link Timestamp} object. For new code prefer {@link LocalDateTime} or
     * {@link OffsetDateTime}.
     */
    @Test
    @SneakyThrows
    public void legacyTimestamp() {
        Timestamp ts = Timestamp.valueOf("2024-06-15 14:30:00.0");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT (?::timestamp) AS ts")) {
                pstmt.setTimestamp(1, ts);

                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();

                    Timestamp result = rs.getTimestamp("ts");
                    assertThat(result).isNotNull();

                    log.info("Legacy Timestamp: written={}, read={}", ts, result);
                }
            }
        }
    }

    /**
     * Session timezone: control how TIMESTAMPTZ values are rendered in queries.
     *
     * <p>Setting {@code querySetting.time_zone} affects how Hyper interprets and displays
     * timestamp literals in queries, but {@link OffsetDateTime} roundtrip always preserves the
     * UTC epoch regardless of session timezone.
     */
    @Test
    @SneakyThrows
    public void sessionTimezoneDoesNotAffectOffsetDateTimeRoundtrip() {
        OffsetDateTime eventTime = OffsetDateTime.of(2024, 6, 15, 21, 30, 45, 0, ZoneOffset.UTC);

        Properties props = new Properties();
        props.setProperty("querySetting.time_zone", "America/New_York");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(props)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT (?::timestamptz) AS ts")) {
                pstmt.setObject(1, eventTime);

                try (ResultSet rs = pstmt.executeQuery()) {
                    assertThat(rs.next()).isTrue();

                    // UTC epoch is always preserved — session TZ does not shift it
                    OffsetDateTime result = rs.getObject("ts", OffsetDateTime.class);
                    assertThat(result.toInstant()).isEqualTo(eventTime.toInstant());

                    log.info("Session TZ=New_York, written={}, read={} (instant preserved)", eventTime, result);
                }
            }
        }
    }
}
