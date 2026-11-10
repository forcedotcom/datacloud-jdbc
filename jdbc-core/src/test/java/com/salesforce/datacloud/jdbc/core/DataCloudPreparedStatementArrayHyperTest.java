/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
class DataCloudPreparedStatementArrayHyperTest {

    @Test
    void unnestsParallelArraysIntoRows() throws SQLException {
        LocalDateTime wallClock = LocalDateTime.of(2026, 11, 10, 7, 5, 52);
        Timestamp instant = Timestamp.from(Instant.parse("2026-11-10T15:05:52Z"));
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT * FROM unnest(?, ?, ?, ?, ?) AS t(id, name, amount, ts, tstz) ORDER BY id")) {
            statement.setArray(1, connection.createArrayOf("BIGINT", new Object[] {1L, 2L}));
            statement.setArray(2, connection.createArrayOf("VARCHAR", new Object[] {"it's", null}));
            statement.setArray(
                    3, connection.createArrayOf("DECIMAL", new Object[] {new BigDecimal("1.5"), new BigDecimal("-22.25")
                    }));
            statement.setArray(4, connection.createArrayOf("TIMESTAMP", new Object[] {wallClock, null}));
            statement.setArray(5, connection.createArrayOf("TIMESTAMP_WITH_TIMEZONE", new Object[] {instant, null}));

            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("id")).isEqualTo(1L);
                assertThat(rs.getString("name")).isEqualTo("it's");
                assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo("1.5");
                assertThat(rs.getObject("ts", LocalDateTime.class)).isEqualTo(wallClock);
                assertThat(rs.getTimestamp("tstz").toInstant()).isEqualTo(instant.toInstant());

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("id")).isEqualTo(2L);
                assertThat(rs.getString("name")).isNull();
                assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo("-22.25");
                assertThat(rs.getObject("ts")).isNull();
                assertThat(rs.getObject("tstz")).isNull();

                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void bindsEmptyAndNullArrays() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT (SELECT count(*) FROM unnest(?)) AS empty_count, ? IS NULL AS is_null")) {
            statement.setArray(1, connection.createArrayOf("VARCHAR", new Object[0]));
            statement.setArray(2, null);
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("empty_count")).isZero();
                assertThat(rs.getBoolean("is_null")).isTrue();
            }
        }
    }

    @Test
    void bindsArraysThroughSetObjectAndNamedParameters() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ? AS a")) {
                statement.setObject(1, connection.createArrayOf("INTEGER", new Object[] {4, null, 6}));
                try (ResultSet rs = statement.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    Array result = rs.getArray(1);
                    assertThat(result.getBaseType()).isEqualTo(Types.INTEGER);
                    assertThat((Object[]) result.getArray()).containsExactly(4, null, 6);
                }
            }
            try (DataCloudNamedParameterStatement statement = connection.prepareNamedParameterStatement(
                    "SELECT count(*) FROM unnest(:ids) AS t(id) WHERE id > :min")) {
                statement.setObject("ids", connection.createArrayOf("BIGINT", new Object[] {5L, 15L, 25L}));
                statement.setLong("min", 10L);
                try (ResultSet rs = statement.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(2L);
                }
            }
        }
    }

    @Test
    void bindsJavaArraysThroughSetObject() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT * FROM unnest(?, ?, ?) AS t(id, name, score) ORDER BY id")) {
            statement.setObject(1, new long[] {1L, 2L});
            statement.setObject(2, new String[] {"a", null});
            statement.setObject(3, new Double[] {0.5, 1.5});
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("id")).isEqualTo(1L);
                assertThat(rs.getString("name")).isEqualTo("a");
                assertThat(rs.getDouble("score")).isEqualTo(0.5);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("name")).isNull();
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void namesTheFailingElement() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM unnest(?)")) {
            statement.setArray(1, connection.createArrayOf("VARCHAR", new Object[] {"ok", 42}));
            assertThatThrownBy(statement::executeQuery)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("array element 1");
        }
    }

    @Test
    void rejectsUnsupportedJavaArrayElementTypes() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            assertThatThrownBy(() -> statement.setObject(1, new Object[] {1}))
                    .isInstanceOf(SQLFeatureNotSupportedException.class);
        }
    }

    @Test
    void rejectsUnsupportedElementTypes() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            assertThatThrownBy(() -> connection.createArrayOf("geography", new Object[0]))
                    .isInstanceOf(SQLFeatureNotSupportedException.class);
            Array nested = connection.createArrayOf("ARRAY", new Object[0]);
            assertThatThrownBy(() -> statement.setArray(1, nested)).isInstanceOf(SQLFeatureNotSupportedException.class);
        }
    }
}
