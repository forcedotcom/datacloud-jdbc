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
    void roundTripsArraysReadFromAResultSet() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement query = connection.prepareStatement("SELECT * FROM unnest(?)");
                PreparedStatement source = connection.prepareStatement(
                        "SELECT ARRAY['a', 'b']::text[] AS s, ARRAY[DATE '2026-11-10']::date[] AS d,"
                                + " ARRAY[TIMESTAMPTZ '2026-11-10 15:05:52+00']::timestamptz[] AS tz,"
                                + " ARRAY[TIMESTAMP '2026-11-10 07:05:52']::timestamp[] AS ts")) {
            try (ResultSet rs = source.executeQuery()) {
                assertThat(rs.next()).isTrue();
                for (int column = 1; column <= 4; column++) {
                    Array fetched = rs.getArray(column);
                    query.setArray(1, fetched);
                    try (ResultSet echoed = query.executeQuery()) {
                        for (Object expected : (Object[]) fetched.getArray()) {
                            assertThat(echoed.next()).isTrue();
                            assertThat(echoed.getObject(1)).isEqualTo(expected);
                        }
                        assertThat(echoed.next()).isFalse();
                    }
                }
            }
        }
    }

    @Test
    void bindsPrimitiveArraysReturnedByForeignArrays() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT sum(x) FROM unnest(?) AS t(x)")) {
            Array foreign = (Array) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {Array.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getBaseType":
                                return Types.INTEGER;
                            case "getBaseTypeName":
                                return "INTEGER";
                            case "getArray":
                                return new int[] {1, 2, 3};
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
            statement.setArray(1, foreign);
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(6L);
            }
        }
    }

    @Test
    void acceptsSqlTypeNames() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement =
                        connection.prepareStatement("SELECT * FROM unnest(?, ?, ?) AS t(a, b, c)")) {
            statement.setArray(1, connection.createArrayOf("text", new Object[] {"x"}));
            statement.setArray(2, connection.createArrayOf("double precision", new Object[] {1.5d}));
            statement.setArray(
                    3, connection.createArrayOf("timestamp with time zone", new Object[] {new Timestamp(0)}));
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("a")).isEqualTo("x");
                assertThat(rs.getDouble("b")).isEqualTo(1.5d);
                assertThat(rs.getTimestamp("c")).isEqualTo(new Timestamp(0));
            }
        }
    }

    @Test
    void sizesDecimalsWithoutAnExtraIntegerDigit() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM unnest(?) AS t(x)")) {
            statement.setArray(1, connection.createArrayOf("DECIMAL", new Object[] {new BigDecimal("1E-38"), null}));
            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBigDecimal(1)).isEqualByComparingTo("1E-38");
            }
        }
    }

    @Test
    void rejectsElementsThatDoNotMatchTheArrayType() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection()) {
            assertThatThrownBy(() -> connection.createArrayOf("INTEGER", new String[] {"a"}))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("array element 0");
        }
    }

    @Test
    void namesTheFailingElement() throws SQLException {
        try (DataCloudConnection connection = getHyperQueryConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM unnest(?)")) {
            statement.setArray(1, connection.createArrayOf("TIMESTAMP", new Object[] {new Timestamp(0), "x"}));
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
