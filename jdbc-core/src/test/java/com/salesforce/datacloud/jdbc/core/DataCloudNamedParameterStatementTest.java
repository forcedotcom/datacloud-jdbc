/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterBinding;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class DataCloudNamedParameterStatementTest extends InterceptedHyperTestBase {
    private DataCloudNamedParameterStatement preparedStatement;

    @BeforeEach
    void setUp() {
        preparedStatement = getInterceptedClientConnection()
                .prepareNamedParameterStatement(
                        "SELECT :left_value + :right_value AS total, :left_value AS left_value");
    }

    @Test
    void bindsParametersByName() throws SQLException {
        preparedStatement.setInt("right_value", 2);
        preparedStatement.setInt("left_value", 40);
        preparedStatement.setInt("right_value", 3);

        assertThat(preparedStatement.parameters)
                .hasSize(2)
                .containsEntry("right_value", new ParameterBinding(HyperType.int32(false), 3))
                .containsEntry("left_value", new ParameterBinding(HyperType.int32(false), 40));
    }

    @Test
    void acceptsExactQuotedParameterName() throws SQLException {
        preparedStatement.setString("order total", "value");

        assertThat(preparedStatement.parameters)
                .containsEntry("order total", new ParameterBinding(HyperType.varcharUnlimited(true), "value"));
    }

    @Test
    void rejectsEmptyParameterNames() {
        assertThatThrownBy(() -> preparedStatement.setString("", "value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter name must not be null or empty");
        assertThatThrownBy(() -> preparedStatement.setString(null, "value"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Parameter name must not be null or empty");
    }

    @Test
    void clearParametersRemovesNamedBindings() throws SQLException {
        preparedStatement.setInt("left_value", 40);

        preparedStatement.clearParameters();

        assertThat(preparedStatement.parameters).isEmpty();
    }

    @Test
    void setObjectUsesSharedTypeMapping() throws SQLException {
        preparedStatement.setObject("left_value", 40);
        preparedStatement.setObject("right_value", null, Types.INTEGER);

        assertThat(preparedStatement.parameters)
                .containsEntry("left_value", new ParameterBinding(HyperType.int32(false), 40))
                .containsEntry("right_value", new ParameterBinding(HyperType.int32(true), null));
    }

    @ParameterizedTest
    @ValueSource(
            ints = {
                Types.BOOLEAN, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.REAL, Types.DOUBLE,
                Types.DECIMAL, Types.VARCHAR, Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE
            })
    void setObjectNullRetainsRequestedJdbcType(int sqlType) throws SQLException {
        try (DataCloudNamedParameterStatement statement =
                getInterceptedClientConnection().prepareNamedParameterStatement("SELECT :bound_value AS value")) {
            statement.setObject("bound_value", null, sqlType);

            try (java.sql.ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.getMetaData().getColumnType(1)).isEqualTo(sqlType);
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getObject(1)).isNull();
                assertThat(resultSet.wasNull()).isTrue();
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    void nullTimestampRetainsTimestampType() throws SQLException {
        preparedStatement.setTimestamp("left_value", null);
        preparedStatement.setTimestamp("right_value", null, java.util.Calendar.getInstance());

        assertThat(preparedStatement.parameters)
                .containsEntry("left_value", new ParameterBinding(HyperType.timestamp(true), null))
                .containsEntry("right_value", new ParameterBinding(HyperType.timestamp(true), null));
    }

    @Test
    void nullCalendarDateAndTimeRetainTheirTypes() throws SQLException {
        preparedStatement.setDate("left_value", null, java.util.Calendar.getInstance());
        preparedStatement.setTime("right_value", null, java.util.Calendar.getInstance());

        assertThat(preparedStatement.parameters)
                .containsEntry("left_value", new ParameterBinding(HyperType.date(true), null))
                .containsEntry("right_value", new ParameterBinding(HyperType.time(true), null));
    }

    @Test
    void rejectsStatementSqlOverrides() {
        assertThatThrownBy(() -> preparedStatement.executeQuery("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("use DataCloudNamedParameterStatement::executeQuery() instead");
        assertThatThrownBy(() -> preparedStatement.execute("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("use DataCloudNamedParameterStatement::execute() instead");
        assertThatThrownBy(() -> preparedStatement.executeAsyncQuery("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("use DataCloudNamedParameterStatement::executeAsyncQuery() instead");
    }

    @Test
    void executesNamedParametersAgainstHyper() throws SQLException {
        preparedStatement.setInt("left_value", 40);
        preparedStatement.setInt("right_value", 2);

        try (java.sql.ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt("total")).isEqualTo(42);
            assertThat(resultSet.getInt("left_value")).isEqualTo(40);
            assertThat(resultSet.next()).isFalse();
        }
    }

    @Test
    void executesScalarParametersAgainstHyper() throws SQLException {
        try (DataCloudNamedParameterStatement statement = getInterceptedClientConnection()
                .prepareNamedParameterStatement(
                        "SELECT :bool_value AS bool_value, :byte_value AS byte_value, :short_value AS short_value, "
                                + ":long_value AS long_value, :float_value AS float_value, :double_value AS double_value, "
                                + ":date_value AS date_value, :time_value AS time_value")) {
            statement.setBoolean("bool_value", true);
            statement.setByte("byte_value", (byte) 7);
            statement.setShort("short_value", (short) 32000);
            statement.setLong("long_value", 5_000_000_000L);
            statement.setFloat("float_value", 1.5f);
            statement.setDouble("double_value", 2.5);
            statement.setDate("date_value", Date.valueOf("2024-01-02"));
            statement.setTime("time_value", Time.valueOf("12:34:56"));

            try (java.sql.ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getBoolean("bool_value")).isTrue();
                assertThat(resultSet.getByte("byte_value")).isEqualTo((byte) 7);
                assertThat(resultSet.getShort("short_value")).isEqualTo((short) 32000);
                assertThat(resultSet.getLong("long_value")).isEqualTo(5_000_000_000L);
                assertThat(resultSet.getFloat("float_value")).isEqualTo(1.5f);
                assertThat(resultSet.getDouble("double_value")).isEqualTo(2.5);
                assertThat(resultSet.getDate("date_value")).isEqualTo(Date.valueOf("2024-01-02"));
                assertThat(resultSet.getTime("time_value")).isEqualTo(Time.valueOf("12:34:56"));
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    private static Stream<Arguments> scalarObjects() {
        return Stream.of(
                Arguments.of(new BigDecimal("12.34"), new BigDecimal("12.34")),
                Arguments.of((short) 7, 7),
                Arguments.of(5_000_000_000L, 5_000_000_000L),
                Arguments.of(1.5f, 1.5f),
                Arguments.of(2.5, 2.5),
                Arguments.of(Date.valueOf("2024-01-02"), Date.valueOf("2024-01-02")),
                Arguments.of(Time.valueOf("12:34:56"), Time.valueOf("12:34:56")),
                Arguments.of(true, true));
    }

    @ParameterizedTest
    @MethodSource("scalarObjects")
    void executesInferredScalarObjectsAgainstHyper(Object value, Object expectedValue) throws SQLException {
        try (DataCloudNamedParameterStatement statement =
                getInterceptedClientConnection().prepareNamedParameterStatement("SELECT :bound_value AS value")) {
            statement.setObject("bound_value", value);

            assertThat(statement.execute()).isTrue();
            try (java.sql.ResultSet resultSet = statement.getResultSet()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getObject(1)).isEqualTo(expectedValue);
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    void executesQuotedParameterNameContainingSpacesAgainstHyper() throws SQLException {
        try (DataCloudNamedParameterStatement statement =
                getInterceptedClientConnection().prepareNamedParameterStatement("SELECT :\"order total\" AS total")) {
            statement.setInt("order total", 42);

            try (java.sql.ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getInt("total")).isEqualTo(42);
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    void executesDecimalNullsAgainstHyper() throws SQLException {
        try (DataCloudNamedParameterStatement statement = getInterceptedClientConnection()
                .prepareNamedParameterStatement(
                        "SELECT :setter_null IS NULL AS setter_null, :typed_null IS NULL AS typed_null, "
                                + ":untyped_null IS NULL AS untyped_null")) {
            statement.setBigDecimal("setter_null", null);
            statement.setNull("typed_null", Types.DECIMAL);
            statement.setObject("untyped_null", null);

            try (java.sql.ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getBoolean("setter_null")).isTrue();
                assertThat(resultSet.getBoolean("typed_null")).isTrue();
                assertThat(resultSet.getBoolean("untyped_null")).isTrue();
            }
        }
    }

    @Test
    void rejectsNullTypesWithoutParameterEncodingSupport() {
        int[] unsupportedTypes = {
            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.TIME_WITH_TIMEZONE, Types.ARRAY
        };

        for (int unsupportedType : unsupportedTypes) {
            assertThatThrownBy(() -> preparedStatement.setNull("value", unsupportedType))
                    .isInstanceOf(SQLFeatureNotSupportedException.class)
                    .hasFieldOrPropertyWithValue("SQLState", "HYC00")
                    .hasMessageContaining("JDBC type " + unsupportedType);
            assertThatThrownBy(() -> preparedStatement.setObject("value", null, unsupportedType))
                    .isInstanceOf(SQLFeatureNotSupportedException.class)
                    .hasFieldOrPropertyWithValue("SQLState", "HYC00")
                    .hasMessageContaining("JDBC type " + unsupportedType);
        }
    }

    @Test
    void rejectsUnknownJdbcTypeCodes() {
        assertThatThrownBy(() -> preparedStatement.setObject("value", null, Integer.MIN_VALUE))
                .isInstanceOf(SQLException.class)
                .hasFieldOrPropertyWithValue("SQLState", "HYC00")
                .hasMessageContaining("Unsupported JDBC type code: " + Integer.MIN_VALUE);
    }

    @Test
    void reportsParameterEncodingFailuresAsSqlExceptions() throws SQLException {
        try (DataCloudNamedParameterStatement statement =
                getInterceptedClientConnection().prepareNamedParameterStatement("SELECT :value")) {
            statement.setObject("value", "not an integer", Types.INTEGER);

            assertThatThrownBy(statement::executeQuery)
                    .isInstanceOf(SQLException.class)
                    .hasFieldOrPropertyWithValue("SQLState", "HY000")
                    .hasMessageContaining("Failed to encode parameters on prepared statement");
        }
    }
}
