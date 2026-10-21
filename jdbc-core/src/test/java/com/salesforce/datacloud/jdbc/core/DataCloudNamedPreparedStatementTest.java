/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.ParameterBinding;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataCloudNamedPreparedStatementTest extends InterceptedHyperTestBase {
    private DataCloudNamedPreparedStatement preparedStatement;

    @BeforeEach
    void setUp() {
        preparedStatement = getInterceptedClientConnection()
                .prepareNamedStatement("SELECT :left_value + :right_value AS total, :left_value AS left_value");
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
                .containsEntry("right_value", new ParameterBinding(HyperType.nullType(), null));
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
                .hasMessageContaining("use DataCloudNamedPreparedStatement::executeQuery() instead");
        assertThatThrownBy(() -> preparedStatement.execute("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("use DataCloudNamedPreparedStatement::execute() instead");
        assertThatThrownBy(() -> preparedStatement.executeAsyncQuery("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("use DataCloudNamedPreparedStatement::executeAsyncQuery() instead");
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
    void executesFloatParameterAgainstHyper() throws SQLException {
        try (DataCloudNamedPreparedStatement statement =
                getInterceptedClientConnection().prepareNamedStatement("SELECT :float_value AS float_value")) {
            statement.setFloat("float_value", 1.5f);

            try (java.sql.ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getFloat("float_value")).isEqualTo(1.5f);
            }
        }
    }

    @Test
    void executesQuotedParameterNameContainingSpacesAgainstHyper() throws SQLException {
        try (DataCloudNamedPreparedStatement statement =
                getInterceptedClientConnection().prepareNamedStatement("SELECT :\"order total\" AS total")) {
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
        try (DataCloudNamedPreparedStatement statement = getInterceptedClientConnection()
                .prepareNamedStatement("SELECT :setter_null IS NULL AS setter_null, :typed_null IS NULL AS typed_null, "
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
        }
    }

    @Test
    void reportsParameterEncodingFailuresAsSqlExceptions() throws SQLException {
        try (DataCloudNamedPreparedStatement statement =
                getInterceptedClientConnection().prepareNamedStatement("SELECT :value")) {
            statement.setObject("value", "not an integer", Types.INTEGER);

            assertThatThrownBy(statement::executeQuery)
                    .isInstanceOf(SQLException.class)
                    .hasFieldOrPropertyWithValue("SQLState", "HY000")
                    .hasMessageContaining("Failed to encode parameters on prepared statement");
        }
    }
}
