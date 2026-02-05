/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import com.salesforce.datacloud.jdbc.util.HyperLogScope;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(LocalHyperTestBase.class)
public class DataCloudPreparedStatementHyperTest {
    @Test
    @SneakyThrows
    public void testPreparedStatementDateRange() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 5);

        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("select ? as a")) {
                for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                    val sqlDate = Date.valueOf(date);
                    preparedStatement.setDate(1, sqlDate);

                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            assertThat(resultSet.getDate("a"))
                                    .isEqualTo(sqlDate)
                                    .as("Expected the date to be %s but got %s", sqlDate, resultSet.getDate("a"));
                        }
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testPreparedStatementDateWithCalendarRange() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 5);

        TimeZone plusTwoTimeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(plusTwoTimeZone);

        TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("select ? as a")) {
                for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                    val sqlDate = Date.valueOf(date);
                    preparedStatement.setDate(1, sqlDate, calendar);

                    val time = sqlDate.getTime();

                    val dateTime = new Timestamp(time).toLocalDateTime();

                    val zonedDateTime = dateTime.atZone(plusTwoTimeZone.toZoneId());
                    val convertedDateTime = zonedDateTime.withZoneSameInstant(utcTimeZone.toZoneId());
                    val expected =
                            Date.valueOf(convertedDateTime.toLocalDateTime().toLocalDate());

                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            val actual = resultSet.getDate("a");
                            assertThat(actual)
                                    .isEqualTo(expected)
                                    .as("Expected the date to be %s in UTC timezone but got %s", sqlDate, actual);
                        }
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testPreparedStatementTimeRange() {
        LocalTime startTime = LocalTime.of(10, 0, 0, 0);
        LocalTime endTime = LocalTime.of(15, 0, 0, 0);

        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("select ? as a")) {
                for (LocalTime time = startTime; !time.isAfter(endTime); time = time.plusHours(1)) {
                    val sqlTime = Time.valueOf(time);
                    preparedStatement.setTime(1, sqlTime);

                    try (val resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            Time actual = resultSet.getTime("a");
                            assertThat(actual)
                                    .isEqualTo(sqlTime)
                                    .as("Expected the date to be %s but got %s", sqlTime, actual);
                        }
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testPreparedStatementTimeWithCalendarRange() {
        LocalTime startTime = LocalTime.of(10, 0, 0, 0);
        LocalTime endTime = LocalTime.of(15, 0, 0, 0);

        TimeZone plusTwoTimeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(plusTwoTimeZone);

        TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("select ? as a")) {
                for (LocalTime time = startTime; !time.isAfter(endTime); time = time.plusHours(1)) {

                    val sqlTime = Time.valueOf(time);
                    preparedStatement.setTime(1, sqlTime, calendar);

                    val dateTime = new Timestamp(sqlTime.getTime()).toLocalDateTime();

                    val zonedDateTime = dateTime.atZone(plusTwoTimeZone.toZoneId());
                    val convertedDateTime = zonedDateTime.withZoneSameInstant(utcTimeZone.toZoneId());
                    val expected = Time.valueOf(convertedDateTime.toLocalTime());

                    try (val resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            val actual = resultSet.getTime("a");
                            assertThat(actual)
                                    .isEqualTo(expected)
                                    .as("Expected the date to be %s in UTC timezone but got %s", sqlTime, actual);
                        }
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testPreparedStatementTimestampRange() {
        LocalDateTime startDateTime = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime endDateTime = LocalDateTime.of(2024, 1, 5, 0, 0);

        TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
        Calendar utcCalendar = Calendar.getInstance(utcTimeZone);

        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("select ? as a")) {
                for (LocalDateTime dateTime = startDateTime;
                        !dateTime.isAfter(endDateTime);
                        dateTime = dateTime.plusDays(1)) {
                    val sqlTimestamp = Timestamp.valueOf(dateTime);
                    preparedStatement.setTimestamp(1, sqlTimestamp);

                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            val actual = resultSet.getTimestamp("a", utcCalendar);
                            assertThat(actual)
                                    .isEqualTo(sqlTimestamp)
                                    .as("Expected the date to be %s in UTC timezone but got %s", sqlTimestamp, actual);
                        }
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testPreparedStatementTimestampWithCalendarRange() {
        LocalDateTime startDateTime = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime endDateTime = LocalDateTime.of(2024, 1, 5, 0, 0);

        TimeZone plusTwoTimeZone = TimeZone.getTimeZone("GMT+2");
        Calendar calendar = Calendar.getInstance(plusTwoTimeZone);

        TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
        Calendar utcCalendar = Calendar.getInstance(utcTimeZone);

        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("select ? as a")) {
                for (LocalDateTime dateTime = startDateTime;
                        !dateTime.isAfter(endDateTime);
                        dateTime = dateTime.plusDays(1)) {

                    val sqlTimestamp = Timestamp.valueOf(dateTime);
                    preparedStatement.setTimestamp(1, sqlTimestamp, calendar);

                    val localDateTime = sqlTimestamp.toLocalDateTime();

                    val zonedDateTime = localDateTime.atZone(plusTwoTimeZone.toZoneId());
                    val convertedDateTime = zonedDateTime.withZoneSameInstant(utcTimeZone.toZoneId());
                    val expected = Timestamp.valueOf(convertedDateTime.toLocalDateTime());

                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            val actual = resultSet.getTimestamp("a", utcCalendar);
                            assertThat(actual)
                                    .isEqualTo(expected)
                                    .as("Expected the date to be %s in UTC timezone but got %s", sqlTimestamp, actual);
                        }
                    }
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetMetaDataReturnsResultSetMetaData() {
        try (HyperLogScope logScope = new HyperLogScope()) {
            try (Connection connection = getHyperQueryConnection(logScope.getProperties())) {
                try (PreparedStatement preparedStatement =
                        connection.prepareStatement("select 1 as id, 'test' as name, 3.14 as value, pg_sleep(100000) as"
                                + " would_timeout_in_execution")) {
                    ResultSetMetaData metadata = preparedStatement.getMetaData();

                    assertThat(metadata).isNotNull();
                    assertThat(metadata.getColumnCount()).isEqualTo(4);

                    assertThat(metadata.getColumnName(1)).isEqualTo("id");
                    assertThat(metadata.getColumnTypeName(1)).isEqualTo("INTEGER");

                    assertThat(metadata.getColumnName(2)).isEqualTo("name");
                    assertThat(metadata.getColumnTypeName(2)).isEqualTo("VARCHAR");

                    assertThat(metadata.getColumnName(3)).isEqualTo("value");
                    assertThat(metadata.getColumnTypeName(3)).isEqualTo("DECIMAL");

                    // Verify that the query actually finished
                    ResultSet resultSet = logScope.executeQuery("SELECT COUNT(*) FROM hyper_log WHERE k='query-end'");
                    resultSet.next();
                    Assertions.assertThat(resultSet.getDouble(1)).isEqualTo(1);
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetMetaDataFollowedByExecuteReturnsData() {
        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement =
                    connection.prepareStatement("select 1 as id, 'test' as name, 3.14 as value")) {
                ResultSetMetaData metadata = preparedStatement.getMetaData();
                assertThat(metadata).isNotNull();
                assertThat(metadata.getColumnCount()).isEqualTo(3);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getInt("id")).isEqualTo(1);
                    assertThat(resultSet.getString("name")).isEqualTo("test");
                    assertThat(resultSet.getBigDecimal("value")).isEqualTo(BigDecimal.valueOf(3.14));
                    assertThat(resultSet.next()).isFalse();
                }
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetMetaDataWithInvalidQueryThrowsSQLException() {
        try (Connection connection = getHyperQueryConnection()) {
            try (PreparedStatement preparedStatement =
                    connection.prepareStatement("select * from non_existent_table")) {
                Assertions.assertThatThrownBy(preparedStatement::getMetaData)
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("table \"non_existent_table\" does not exist");
            }
        }
    }

    @Test
    @SneakyThrows
    public void testGetMetaDataAfterExecuteDoesNotQueryAgain() {
        try (HyperLogScope logScope = new HyperLogScope()) {
            try (Connection connection = getHyperQueryConnection(logScope.getProperties())) {
                try (PreparedStatement preparedStatement =
                        connection.prepareStatement("select 1 as id, 'test' as name")) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        assertThat(resultSet.next()).isTrue();

                        ResultSetMetaData metadata = preparedStatement.getMetaData();
                        assertThat(metadata).isNotNull();
                        assertThat(metadata.getColumnCount()).isEqualTo(2);
                    }

                    ResultSet logResult = logScope.executeQuery("SELECT COUNT(*) FROM hyper_log WHERE k='query-end'");
                    logResult.next();
                    Assertions.assertThat(logResult.getDouble(1))
                            .as("Should only have one query execution, not two")
                            .isEqualTo(1);

                    // Test that after closing the resultset it would query again
                    ResultSetMetaData metadata = preparedStatement.getMetaData();
                    assertThat(metadata).isNotNull();
                    assertThat(metadata.getColumnCount()).isEqualTo(2);

                    ResultSet logResult2 = logScope.executeQuery("SELECT COUNT(*) FROM hyper_log WHERE k='query-end'");
                    logResult2.next();
                    Assertions.assertThat(logResult2.getDouble(1)).isEqualTo(2);
                }
            }
        }
    }
}
