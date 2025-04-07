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
package com.salesforce.datacloud.jdbc;

import static com.salesforce.datacloud.jdbc.core.DataCloudConnectionString.CONNECTION_PROTOCOL;
import static com.salesforce.datacloud.jdbc.core.StreamingResultSetTest.query;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import com.salesforce.datacloud.jdbc.auth.AuthenticationSettings;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.util.ThrowingBiFunction;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * To run this test, set the environment variables for the various AuthenticationSettings strategies. Right-click the
 * play button and click "modify run configuration" and paste the following in "Environment Variables" Then you can
 * click the little icon on the right side of the field to update the values appropriately.
 * loginURL=login.salesforce.com/;userName=xyz@salesforce.com;password=...;clientId=...;clientSecret=...;query=SELECT
 * "Description__c" FROM Account_Home__dll LIMIT 100
 */
@Slf4j
@Value
@EnabledIf("validateProperties")
class OrgIntegrationTest {
    static Properties getPropertiesFromEnvironment() {
        val properties = new Properties();
        System.getenv().entrySet().stream()
                .filter(e -> SETTINGS.contains(e.getKey()))
                .forEach(e -> properties.setProperty(e.getKey(), e.getValue()));
        return properties;
    }

    static AuthenticationSettings getSettingsFromEnvironment() {
        try {
            return AuthenticationSettings.of(getPropertiesFromEnvironment());
        } catch (Exception e) {
            return null;
        }
    }

    @Getter(lazy = true)
    Properties properties = getPropertiesFromEnvironment();

    @Getter(lazy = true)
    AuthenticationSettings settings = getSettingsFromEnvironment();

    private static final int NUM_THREADS = 100;

    @Test
    @SneakyThrows
    @Disabled
    void testDatasource() {
        val query = "SELECT * FROM Account_Home__dll LIMIT 100";
        val connectionUrl = CONNECTION_PROTOCOL + getSettings().getLoginUrl();
        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");
        DataCloudDatasource datasource = new DataCloudDatasource();
        datasource.setConnectionUrl(connectionUrl);
        datasource.setUserName(getProperties().getProperty("userName"));
        datasource.setPassword(getProperties().getProperty("password"));
        datasource.setClientId(getProperties().getProperty("clientId"));
        datasource.setClientSecret(getProperties().getProperty("clientSecret"));

        try (val connection = datasource.getConnection();
                val statement = connection.createStatement()) {
            val resultSet = statement.executeQuery(query);
            assertThat(resultSet.next()).isTrue();
        }

        assertThrows(SQLException.class, () -> datasource.getConnection("foo", "bar"));
    }

    @Test
    @SneakyThrows
    @Disabled
    void testMetadata() {
        try (val connection = getConnection()) {
            val tableName = getProperties().getProperty("tableName", "Account_Home__dll");
            ResultSet columnResultSet = connection.getMetaData().getColumns("", "public", tableName, null);
            ResultSet tableResultSet = connection.getMetaData().getTables(null, null, "%", null);
            ResultSet schemaResultSetWithCatalogAndSchemaPattern =
                    connection.getMetaData().getSchemas(null, "public");
            ResultSet schemaResultSet = connection.getMetaData().getSchemas();
            ResultSet tableTypesResultSet = connection.getMetaData().getTableTypes();
            ResultSet catalogsResultSet = connection.getMetaData().getCatalogs();
            ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(null, null, "Account_Home__dll");
            while (primaryKeys.next()) {
                log.info("trying to print primary keys");
            }

            assertThat(columnResultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
            assertThat(tableResultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
            assertThat(schemaResultSetWithCatalogAndSchemaPattern.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
            assertThat(schemaResultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
            assertThat(tableTypesResultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
            assertThat(catalogsResultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
        }
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("com.salesforce.datacloud.jdbc.core.StreamingResultSetTest#queryModesWithMax")
    public void exerciseQueryMode(
            ThrowingBiFunction<DataCloudStatement, String, DataCloudResultSet> queryMode, int max) {
        val sql = query(Integer.toString(max));
        try (val connection = getConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {
            val rs = queryMode.apply(statement, sql);

            assertThat(rs.isReady()).isTrue();
            assertThat(rs).isInstanceOf(StreamingResultSet.class);

            int expected = 0;
            while (rs.next()) {
                expected++;
            }

            log.info("final value: {}", expected);
            assertThat(expected).isEqualTo(max);
            assertThat(rs.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
        }
    }

    @SneakyThrows
    private DataCloudConnection getConnection() {
        val connectionUrl = CONNECTION_PROTOCOL + getSettings().getLoginUrl();
        log.info("Connection URL: {}", connectionUrl);

        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");
        val connection = DriverManager.getConnection(connectionUrl, getProperties());
        return (DataCloudConnection) connection;
    }

    @SneakyThrows
    @Test
    @Disabled
    public void testPreparedStatementExecuteWithParams() {
        val query =
                "SELECT \"Id__c\", \"AnnualRevenue__c\", \"LastModifiedDate__c\" FROM Account_Home__dll WHERE \"Id__c\" = ? AND \"AnnualRevenue__c\" = ? AND \"LastModifiedDate__c\" = ?";
        try (val connection = getConnection();
                val statement = connection.prepareStatement(query)) {
            val id = "001SB00000K3pP4YAJ";
            val annualRevenue = 100000000;
            val lastModifiedDate = Timestamp.valueOf("2024-06-10 05:07:52.0");
            statement.setString(1, id);
            statement.setInt(2, annualRevenue);
            statement.setTimestamp(3, lastModifiedDate);

            statement.execute(query);
            val resultSet = statement.getResultSet();

            val results = new ArrayList<String>();
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

            while (resultSet.next()) {
                val idResult = resultSet.getString(1);
                val annualRevenueResult = resultSet.getInt(2);
                val lastModifiedDateResult = resultSet.getTimestamp(3, cal);
                log.info("{} : {} : {}", idResult, annualRevenueResult, lastModifiedDateResult);
                assertThat(idResult).isEqualTo(id);
                assertThat(annualRevenueResult).isEqualTo(annualRevenue);
                assertThat(lastModifiedDateResult).isEqualTo(lastModifiedDate);
                val row = resultSet.getRow();
                Optional.ofNullable(resultSet.getObject("Id__c")).ifPresent(t -> results.add(row + " - " + t));
                Optional.ofNullable(resultSet.getObject("AnnualRevenue__c"))
                        .ifPresent(t -> results.add(row + " - " + t));
                Optional.ofNullable(resultSet.getObject("LastModifiedDate__c"))
                        .ifPresent(t -> results.add(row + " - " + t));
            }
            assertThat(results.stream().filter(t -> !Objects.isNull(t))).hasSizeGreaterThanOrEqualTo(0);

            assertThat(resultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
        }
    }

    @SneakyThrows
    @Test
    @Disabled
    public void testPreparedStatementGetResultSetNoParams() {
        val query = "SELECT \"Id__c\", \"AnnualRevenue__c\", \"LastModifiedDate__c\" FROM Account_Home__dll LIMIT 100";
        try (val connection = getConnection();
                val statement = connection.prepareStatement(query)) {
            statement.execute(query);
            val resultSet = statement.getResultSet();

            val results = new ArrayList<String>();

            while (resultSet.next()) {
                val row = resultSet.getRow();
                val resultFromColumnIndex = resultSet.getString(1);
                val resultFromColumnName = resultSet.getString("Id__c");
                val resultFromColumnIndex2 = resultSet.getString(2);
                val resultFromColumnName2 = resultSet.getString("AnnualRevenue__c");
                val resultFromColumnIndex3 = resultSet.getString(3);
                val resultFromColumnName3 = resultSet.getString("LastModifiedDate__c");
                assertThat(resultFromColumnIndex).isEqualTo(resultFromColumnName);
                assertThat(resultFromColumnIndex2).isEqualTo(resultFromColumnName2);
                assertThat(resultFromColumnIndex3).isEqualTo(resultFromColumnName3);
                Optional.ofNullable(resultSet.getObject("Id__c")).ifPresent(t -> results.add(row + " - " + t));
                Optional.ofNullable(resultSet.getObject("AnnualRevenue__c"))
                        .ifPresent(t -> results.add(row + " - " + t));
                Optional.ofNullable(resultSet.getObject("LastModifiedDate__c"))
                        .ifPresent(t -> results.add(row + " - " + t));
            }
            assertThat(results.stream().filter(t -> !Objects.isNull(t))).hasSizeGreaterThanOrEqualTo(1);

            assertThat(resultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
        }
    }

    @SneakyThrows
    @Test
    @Disabled
    public void testPreparedStatementExecuteQueryNoParams() {
        val query = "SELECT \"Id__c\", \"AnnualRevenue__c\", \"LastModifiedDate__c\" FROM Account_Home__dll LIMIT 100";
        try (val connection = getConnection();
                val statement = connection.prepareStatement(query)) {

            val resultSet = statement.executeQuery(query);

            val results = new ArrayList<String>();

            while (resultSet.next()) {
                val row = resultSet.getRow();
                val resultFromColumnIndex = resultSet.getString(1);
                val resultFromColumnName = resultSet.getString("Id__c");
                assertThat(resultFromColumnIndex).isEqualTo(resultFromColumnName);
                Optional.ofNullable(resultSet.getObject("Id__c")).ifPresent(t -> results.add(row + " - " + t));
                Optional.ofNullable(resultSet.getObject("AnnualRevenue__c"))
                        .ifPresent(t -> results.add(row + " - " + t));
                Optional.ofNullable(resultSet.getObject("LastModifiedDate__c"))
                        .ifPresent(t -> results.add(row + " - " + t));
            }
            assertThat(results.stream().filter(t -> !Objects.isNull(t))).hasSizeGreaterThanOrEqualTo(1);

            assertThat(resultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
        }
    }

    @Test
    @SneakyThrows
    @Disabled
    void testArrowFieldConversion() {
        Map<Integer, String> queries = new HashMap<>();
        queries.put(Types.BOOLEAN, "SELECT 5 > 100  AS \"boolean_output\"");
        queries.put(Types.VARCHAR, "SELECT 'a test string' as \"string_column\"");
        queries.put(Types.DATE, "SELECT current_date");
        queries.put(Types.TIME, "SELECT current_time");
        queries.put(Types.TIMESTAMP, "SELECT current_timestamp");
        queries.put(Types.DECIMAL, "SELECT 82.3 as  \"decimal_column\"");
        queries.put(Types.INTEGER, "SELECT 82 as  \"Integer_column\"");
        try (val connection = getConnection();
                val statement = connection.createStatement()) {
            for (val entry : queries.entrySet()) {
                val resultSet = statement.executeQuery(entry.getValue());
                val metadata = resultSet.getMetaData();
                log.info("columntypename: {}", metadata.getColumnTypeName(1));
                log.info("columntype: {}", metadata.getColumnType(1));
                Assertions.assertEquals(
                        Integer.toString(metadata.getColumnType(1)),
                        entry.getKey().toString());
            }
        }
    }

    @Test
    @SneakyThrows
    @Disabled
    void testMultiThreadedAuth() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(this::testMainQuery);
        }

        executor.shutdown();
    }

    @Test
    @Disabled
    @SneakyThrows
    void testMainQuery() {
        int max = 100;
        val query = query("100");

        try (val connection = getConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {

            log.info("Begin executeQuery");
            long startTime = System.currentTimeMillis();
            ResultSet resultSet = statement.executeAdaptiveQuery(query);
            log.info("Query executed in {}ms", System.currentTimeMillis() - startTime);

            int expected = 0;
            while (resultSet.next()) {
                expected++;
            }

            log.info("final value: {}", expected);
            assertThat(expected).isEqualTo(max);
            assertThat(resultSet.isClosed())
                    .as("Query ResultSet was closed unexpectedly.")
                    .isFalse();
        }
    }

    static boolean validateProperties() {
        AuthenticationSettings getSettings = getSettingsFromEnvironment();
        return getSettings != null;
    }

    private static final Set<String> SETTINGS = ImmutableSet.of(
            "loginURL",
            "userName",
            "password",
            "privateKey",
            "clientSecret",
            "clientId",
            "dataspace",
            "maxRetries",
            "User-Agent",
            "refreshToken");
}
