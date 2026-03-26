/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.query.v3;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import lombok.val;
import org.junit.jupiter.api.Test;

class QueryExecutionStatisticsTest {
    @Test
    void testOfWithValidProto() {
        val proto = salesforce.cdp.hyperdb.v1.QueryExecutionStatistics.newBuilder()
                .setWallClockTime(1.5)
                .setRowsProcessed(1000)
                .build();

        val result = QueryExecutionStatistics.of(proto);

        assertThat(result).isPresent().get().satisfies(stats -> {
            assertThat(stats.getWallClockTime()).isEqualTo(Duration.ofMillis(1500));
            assertThat(stats.getRowsProcessed()).isEqualTo(1000);
        });
    }

    @Test
    void testOfWithNull() {
        val result = QueryExecutionStatistics.of(null);

        assertThat(result).isEmpty();
    }

    @Test
    void testOfWithZeroValues() {
        val proto = salesforce.cdp.hyperdb.v1.QueryExecutionStatistics.newBuilder()
                .setWallClockTime(0.0)
                .setRowsProcessed(0)
                .build();

        val result = QueryExecutionStatistics.of(proto);

        assertThat(result).isPresent().get().satisfies(stats -> {
            assertThat(stats.getWallClockTime()).isEqualTo(Duration.ZERO);
            assertThat(stats.getRowsProcessed()).isEqualTo(0);
        });
    }

    @Test
    void testOfWithLargeValues() {
        val proto = salesforce.cdp.hyperdb.v1.QueryExecutionStatistics.newBuilder()
                .setWallClockTime(123.456)
                .setRowsProcessed(999999999999L)
                .build();

        val result = QueryExecutionStatistics.of(proto);

        assertThat(result).isPresent().get().satisfies(stats -> {
            assertThat(stats.getWallClockTime()).isEqualTo(Duration.ofNanos(123_456_000_000L));
            assertThat(stats.getRowsProcessed()).isEqualTo(999999999999L);
        });
    }

    @Test
    void testOfPreservesSubSecondPrecision() {
        val proto = salesforce.cdp.hyperdb.v1.QueryExecutionStatistics.newBuilder()
                .setWallClockTime(0.009395458)
                .setRowsProcessed(0)
                .build();

        val result = QueryExecutionStatistics.of(proto);

        assertThat(result).isPresent().get().satisfies(stats -> {
            assertThat(stats.getWallClockTime().toNanos()).isGreaterThan(9_000_000L);
            assertThat(stats.getWallClockTime().toNanos()).isLessThan(10_000_000L);
        });
    }
}
