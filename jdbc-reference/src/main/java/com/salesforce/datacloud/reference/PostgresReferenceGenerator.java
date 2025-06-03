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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL reference generator that connects to a local PostgreSQL server,
 * executes SQL commands loaded from queries.txt file, and generates a reference.json
 * file containing the expected column metadata for each successful query.
 */
public class PostgresReferenceGenerator {

    private static final Logger logger = LoggerFactory.getLogger(PostgresReferenceGenerator.class);

    // Database connection parameters - adjust these for your local setup
    public static final String DB_URL = "jdbc:postgresql://localhost:5432/testdb";
    public static final String DB_USER = "testuser";
    public static final String DB_PASSWORD = "password";

    // Path to the Reference output file in resources
    private static final String REFERENCE_FILE = "reference.json";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        logger.info("Starting PostgreSQL Reference Generator");

        PostgresReferenceGenerator generator = new PostgresReferenceGenerator();
        generator.generateReference();
    }

    /**
     * Generates a reference.json file containing the expected column metadata for each successful query.
     */
    public void generateReference() {
        try {
            // Load the PostgreSQL JDBC driver
            Class.forName("org.postgresql.Driver");
            // Load SQL commands from queries.txt file
            List<ProtocolValue> testValues = ProtocolValue.loadProtocolValues();
            List<String> sqlCommands = testValues.stream()
                    .filter(v -> v.getInterestingness() == ProtocolValue.Interestingness.Null)
                    .map(v -> {
                        if (v.isSimpleType()) {
                            return v.getSql();
                        } else {
                            String typeName = v.getArrayType().getInner().getSqlTypeName();
                            return "select NULL::" + typeName + "[]";
                        }
                    })
                    .collect(Collectors.toList());

            // Establish connection and generate Reference
            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
                logger.info("Connected to PostgreSQL database: {}", DB_URL);

                // Execute SQL commands and collect Reference data
                List<ReferenceEntry> ReferenceEntries = executeSqlAndCollectReference(connection, sqlCommands);

                // Write Reference to JSON file
                writeReferenceToFile(ReferenceEntries);
            } catch (SQLException e) {
                throw new RuntimeException("Database operation failed", e);
            }

        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load PostgreSQL JDBC driver", e);
        } catch (IOException e) {
            throw new RuntimeException("File operation failed", e);
        }
    }

    /**
     * Executes SQL commands and collects reference metadata for successful queries.
     *
     * @param connection the database connection
     * @param sqlCommands list of SQL commands to execute
     * @return list of Reference entries for successful queries
     */
    private List<ReferenceEntry> executeSqlAndCollectReference(Connection connection, List<String> sqlCommands) {
        logger.info("Executing {} SQL commands and collecting Reference metadata", sqlCommands.size());

        List<ReferenceEntry> ReferenceEntries = new ArrayList<>();
        for (int i = 0; i < sqlCommands.size(); i++) {
            String sql = sqlCommands.get(i);
            if (!sql.contains("NULL")) {
                continue;
            }
            logger.debug("Executing SQL command {}: {}", i + 1, sql.substring(0, Math.min(50, sql.length())) + "...");

            try (Statement statement = connection.createStatement()) {
                boolean hasResultSet = statement.execute(sql);

                if (hasResultSet) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        logger.info("Command {} returned result set", i + 1);

                        // Extract metadata for all columns
                        List<ColumnMetadata> columnMetadataList = extractColumnMetadata(resultSet.getMetaData());

                        // Create Reference entry
                        ReferenceEntry ReferenceEntry = new ReferenceEntry(sql, columnMetadataList);
                        ReferenceEntries.add(ReferenceEntry);

                        logger.info(
                                "Added Reference entry for query {} with {} columns", i + 1, columnMetadataList.size());
                    }
                } else {
                    throw new RuntimeException("Query " + i + 1
                            + " did not return a result set. Only SELECT queries are expected. SQL: " + sql);
                }
            } catch (SQLException e) {
                logger.error(
                        "ERROR: Failed to execute SQL command {}. SQL: {} - Error: {}", i + 1, sql, e.getMessage());
            }
        }

        logger.info("Reference collection completed. Total: {}", sqlCommands.size());
        return ReferenceEntries;
    }

    /**
     * Extracts column metadata from ResultSetMetaData.
     *
     * @param metaData the ResultSetMetaData to extract from
     * @return list of ColumnMetadata objects
     * @throws SQLException if metadata extraction fails
     */
    private List<ColumnMetadata> extractColumnMetadata(ResultSetMetaData metaData) throws SQLException {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        int columnCount = metaData.getColumnCount();

        for (int col = 1; col <= columnCount; col++) {
            ColumnMetadata columnMetadata = ColumnMetadata.fromResultSetMetaData(metaData, col);
            columnMetadataList.add(columnMetadata);

            logger.debug("Extracted metadata for column {}: {}", col, columnMetadata);
        }

        return columnMetadataList;
    }

    /**
     * Writes the reference entries to the reference.json file in the resources directory.
     *
     * @param ReferenceEntries list of reference entries to write
     * @throws IOException if file writing fails
     */
    private void writeReferenceToFile(List<ReferenceEntry> referenceEntries) throws IOException {
        // Get the resources directory path
        Path resourcesPath = Paths.get("src", "main", "resources", REFERENCE_FILE);
        System.out.println("Writing Reference to file: " + resourcesPath.toAbsolutePath());

        try {
            // Serialize to JSON with pretty printing
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(resourcesPath.toFile(), referenceEntries);

            logger.info("Successfully wrote {} reference entries to {}", referenceEntries.size(), resourcesPath);
            logger.info("Reference file size: {} entries", referenceEntries.size());
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to serialize reference entries", e);
        }
    }
}
