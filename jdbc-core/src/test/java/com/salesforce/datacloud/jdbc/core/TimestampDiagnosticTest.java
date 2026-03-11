/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Properties;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Diagnostic test to understand how Hyper interprets naive TIMESTAMP values.
 */
@ExtendWith(LocalHyperTestBase.class)
public class TimestampDiagnosticTest {

    @Test
    @SneakyThrows
    public void diagnoseNaiveTimestampBehavior() {
        long epochUTC;
        long epochLA;

        // Test with UTC session timezone
        Properties propsUTC = new Properties();
        propsUTC.setProperty("querySetting.time_zone", "UTC");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(propsUTC)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    Instant instant = rs.getObject("test_timestamp", Instant.class);
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    String str = rs.getString("test_timestamp");

                    epochUTC = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;

                    System.out.println("\n=== UTC Session Timezone ===");
                    System.out.println("Input: TIMESTAMP '2024-03-15 14:30:45.123456'");
                    System.out.println("Instant (UTC):      " + instant);
                    System.out.println("LocalDateTime:      " + ldt);
                    System.out.println("Timestamp:          " + ts);
                    System.out.println("String:             " + str);
                    System.out.println("Epoch Micros:       " + epochUTC);
                }
            }
        }

        // Test with Los Angeles session timezone
        Properties propsLA = new Properties();
        propsLA.setProperty("querySetting.time_zone", "America/Los_Angeles");

        try (DataCloudConnection conn = LocalHyperTestBase.getHyperQueryConnection(propsLA)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT TIMESTAMP '2024-03-15 14:30:45.123456' as test_timestamp";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertThat(rs.next()).isTrue();

                    Instant instant = rs.getObject("test_timestamp", Instant.class);
                    LocalDateTime ldt = rs.getObject("test_timestamp", LocalDateTime.class);
                    Timestamp ts = rs.getTimestamp("test_timestamp");
                    String str = rs.getString("test_timestamp");

                    epochLA = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;

                    System.out.println("\n=== America/Los_Angeles Session Timezone (-07:00) ===");
                    System.out.println("Input: TIMESTAMP '2024-03-15 14:30:45.123456'");
                    System.out.println("Instant (UTC):      " + instant);
                    System.out.println("LocalDateTime:      " + ldt);
                    System.out.println("Timestamp:          " + ts);
                    System.out.println("String:             " + str);
                    System.out.println("Epoch Micros:       " + epochLA);
                }
            }
        }

        System.out.println("\n=== Analysis ===");
        System.out.println("Epoch UTC: " + epochUTC);
        System.out.println("Epoch LA:  " + epochLA);
        System.out.println("Difference: " + (epochLA - epochUTC) + " microseconds ("
                + (epochLA - epochUTC) / 1_000_000 / 3600 + " hours)");
        System.out.println();
        if (epochUTC == epochLA) {
            System.out.println("✓ Epoch values are SAME");
            System.out.println("  → Hyper treats naive TIMESTAMP as UTC (ignores session timezone)");
            System.out.println("  → Bug: We apply timezone conversion when we shouldn't!");
            System.out.println("  → Fix: For naive TIMESTAMP, use UTC zone in getLocalDateTime()");
        } else {
            System.out.println("✓ Epoch values are DIFFERENT");
            System.out.println("  → Hyper interprets naive TIMESTAMP in session timezone");
            System.out.println("  → Our conversion should use same timezone to preserve literals");
        }
    }
}
