/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * To run this test, set the environment variables for the various AuthenticationSettings strategies. Right-click the
 * play button and click "modify run configuration" and paste the following in "Environment Variables".
 * Then you can click the little icon on the right side of the field to update the values appropriately.
 * JDBC_URL=jdbc:salesforce-datacloud://login.salesforce.com?userName=xyz@salesforce.com&password=...&clientId=...&clientSecret=...;
 */
@Slf4j
@Value
@EnabledIf("hasJdbcUrl")
class OrgIntegrationTest {
    static boolean hasJdbcUrl() {
        return System.getenv().containsKey("JDBC_URL");
    }

    static String getJdbcUrlFromEnvironment() {
        String url = System.getenv().get("JDBC_URL");
        if (url == null) {
            throw new IllegalStateException("JDBC_URL environment variable is not set");
        }
        return url;
    }

    String jdbcUrl = getJdbcUrlFromEnvironment();

    private static final int NUM_THREADS = 100;

    @SneakyThrows
    private DataCloudConnection getConnection() {
        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");
        val connection = DriverManager.getConnection(jdbcUrl, null);
        return (DataCloudConnection) connection;
    }

    @Test
    @SneakyThrows
    @Disabled
    void testDatasource() {
        val query = "SELECT * FROM Account_Home__dll LIMIT 100";
        Class.forName("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver");

        try (val connection = DriverManager.getConnection(jdbcUrl, null);
                val statement = connection.createStatement()) {
            val resultSet = statement.executeQuery(query);
            assertThat(resultSet.next()).isTrue();
        }
    }

    @Test
    @SneakyThrows
    @Disabled
    void testMetadata() {
        try (val connection = getConnection()) {
            String tableName = System.getenv().get("TABLE_NAME");
            if (tableName == null) {
                tableName = "Account_Home__dll";
            }

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
        val query =
                "select cast(a as numeric(38,18)) a, cast(a as numeric(38,18)) b, cast(a as numeric(38,18)) c from generate_series(1, 100) as s(a) order by a asc";

        try (val connection = getConnection();
                val statement = connection.createStatement().unwrap(DataCloudStatement.class)) {

            log.info("Begin executeQuery");
            long startTime = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(query);
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
}
