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
package com.salesforce.datacloud.reference;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manual unit test that validates the baseline.json expectations against a live
 * PostgreSQL database.
 * This test requires a PostgreSQL instance to be running with the configured
 * connection parameters.
 *
 * <p>
 * This test is marked as "manual" because it requires external setup:
 * </p>
 * <ul>
 * <li>PostgreSQL server running on localhost:5432</li>
 * <li>Database 'testdb' exists</li>
 * <li>User 'testuser' with password 'password' has access</li>
 * </ul>
 */
@Tag("manual")
public class PostgresReferenceValidationTest {

    private static final Logger logger = LoggerFactory.getLogger(PostgresReferenceValidationTest.class);

    // Database connection parameters - same as baseline generator
    private static final String DB_URL = PostgresReferenceGenerator.DB_URL;
    private static final String DB_USER = PostgresReferenceGenerator.DB_USER;
    private static final String DB_PASSWORD = PostgresReferenceGenerator.DB_PASSWORD;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static List<ReferenceEntry> baselineEntries;

    @BeforeAll
    static void setupClass() throws IOException {
        // Load baseline entries from the JSON file
        baselineEntries = loadReferenceEntries();
        logger.info("Loaded {} baseline entries for validation", baselineEntries.size());
    }

    /**
     * Test that validates PostgreSQL connection is available before running other
     * tests.
     */
    @Test
    @Tag("manual")
    @SneakyThrows
    void testPostgresConnectionAvailable() {
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            assertTrue(connection.isValid(5), "PostgreSQL connection should be valid");
            logger.info("Successfully connected to PostgreSQL database: {}", DB_URL);
        }
    }

    /**
     * Parameterized test that validates each baseline entry against live PostgreSQL
     * results.
     * Each baseline entry contains a SQL query and expected column metadata.
     */
    @ParameterizedTest(name = "Validate baseline entry: {0}")
    @MethodSource("getBaselineEntries")
    @Tag("manual")
    @SneakyThrows
    void testReferenceEntryAgainstPostgres(ReferenceEntry ReferenceEntry) {
        String sql = ReferenceEntry.getQuery();
        List<ColumnMetadata> expectedMetadata = ReferenceEntry.getColumnMetadata();

        logger.debug("Testing query: {}", sql);

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                Statement statement = connection.createStatement()) {

            boolean hasResultSet = statement.execute(sql);
            assertTrue(hasResultSet, "Query should return a result set: " + sql);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData actualMetaData = resultSet.getMetaData();

                // Validate column count
                assertEquals(
                        expectedMetadata.size(),
                        actualMetaData.getColumnCount(),
                        "Column count mismatch for query: " + sql);

                // Validate each column's metadata
                for (int i = 0; i < expectedMetadata.size(); i++) {
                    int columnIndex = i + 1; // JDBC is 1-based
                    ColumnMetadata expected = expectedMetadata.get(i);
                    ColumnMetadata actual = ColumnMetadata.fromResultSetMetaData(actualMetaData, columnIndex);

                    assertEquals(expected, actual);
                }

                logger.debug("Successfully validated metadata for query: {}", sql);
            }
        }
    }

    /**
     * Provides a stream of baseline entries for parameterized testing.
     */
    static Stream<ReferenceEntry> getBaselineEntries() {
        return baselineEntries.stream();
    }

    /**
     * Loads baseline entries from the reference.json resource file.
     */
    private static List<ReferenceEntry> loadReferenceEntries() throws IOException {
        try (InputStream inputStream =
                PostgresReferenceGenerator.class.getClassLoader().getResourceAsStream("reference.json")) {
            if (inputStream == null) {
                throw new IOException("Could not find baseline.json in resources");
            }

            return objectMapper.readValue(inputStream, new TypeReference<List<ReferenceEntry>>() {});
        }
    }
}
