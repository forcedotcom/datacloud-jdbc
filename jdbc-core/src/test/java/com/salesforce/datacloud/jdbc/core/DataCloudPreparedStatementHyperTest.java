/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.getHyperQueryConnection;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(HyperTestBase.class)
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
}
