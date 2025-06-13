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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Represents a baseline entry containing a SQL query, its associated column metadata,
 * and the returned values from query execution.
 * Used for generating baseline expectation files.
 */
@Data
@Builder
@Jacksonized
public class ReferenceEntry {
    private final String query;
    private final List<ColumnMetadata> columnMetadata;

    /**
     * The returned values from the query execution. Each row is represented as a List of ValueWithClass objects,
     * where each ValueWithClass contains both the string representation and the Java class name.
     */
    private final List<List<ValueWithClass>> returnedValues;

    public void validateAgainstResultSet(ResultSet resultSet, String sql) throws SQLException {
        ResultSetMetaData actualMetaData = resultSet.getMetaData();

        // Validate column count
        if (columnMetadata.size() != actualMetaData.getColumnCount()) {
            throw new RuntimeException(String.format(
                    "Column count mismatch for query '%s': expected %d, got %d",
                    sql, columnMetadata.size(), actualMetaData.getColumnCount()));
        }

        // Collect list of differences
        ArrayList<String> differences = new ArrayList<>();
        // Validate each column's metadata
        for (int i = 0; i < columnMetadata.size(); i++) {
            int columnIndex = i + 1; // JDBC is 1-based
            ColumnMetadata expected = columnMetadata.get(i);
            ColumnMetadata actual = ColumnMetadata.fromResultSetMetaData(actualMetaData, columnIndex);
            differences.addAll(actual.collectDifferences(expected));
        }

        // If there are any errors, throw exception with all details, we don't collect row value differences as when the
        // metadata is different
        // the row values likely will be different as well.
        if (differences.size() > 0) {
            throw new IllegalArgumentException("ColumnMetadata validation failed:\n" + String.join("\n", differences));
        }

        // Validate returned values
        List<List<ValueWithClass>> actualValues = extractReturnedValues(resultSet);

        if (returnedValues.size() != actualValues.size()) {
            throw new RuntimeException(String.format(
                    "Row count mismatch for query '%s': expected %d rows, got %d rows",
                    sql, returnedValues.size(), actualValues.size()));
        }

        // Validate each row's values
        for (int rowIndex = 0; rowIndex < returnedValues.size(); rowIndex++) {
            List<ValueWithClass> expectedRow = returnedValues.get(rowIndex);
            List<ValueWithClass> actualRow = actualValues.get(rowIndex);

            if (expectedRow.size() != actualRow.size()) {
                throw new RuntimeException(String.format(
                        "Column count mismatch in row %d for query '%s': expected %d columns, got %d columns",
                        rowIndex + 1, sql, expectedRow.size(), actualRow.size()));
            }

            for (int colIndex = 0; colIndex < expectedRow.size(); colIndex++) {
                ValueWithClass expectedValue = expectedRow.get(colIndex);
                ValueWithClass actualValue = actualRow.get(colIndex);

                // Handle null comparison properly
                if (!expectedValue.equals(actualValue)) {
                    throw new RuntimeException(String.format(
                            "Value mismatch for query '%s', row %d, column %d: expected '%s', got '%s'",
                            sql, rowIndex + 1, colIndex + 1, expectedValue, actualValue));
                }
            }
        }
    }

    /**
     * Extracts returned values from ResultSet using getObject() and preserves both string representation and Java class.
     * Handles null values correctly by preserving them as null in the result.
     *
     * @param resultSet the ResultSet to extract values from
     * @return list of rows, where each row is a list of ValueWithClass objects
     * @throws SQLException if value extraction fails
     */
    public static List<List<ValueWithClass>> extractReturnedValues(ResultSet resultSet) throws SQLException {
        List<List<ValueWithClass>> allRows = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            List<ValueWithClass> rowValues = new ArrayList<>();

            for (int col = 1; col <= columnCount; col++) {
                Object value = resultSet.getObject(col);
                // Also ensure that `wasNull()` is consistent with the value being null
                if (resultSet.wasNull() != (value == null)) {
                    throw new RuntimeException(String.format(
                            "wasNull() returned %s for column %d, but value is %s", resultSet.wasNull(), col, value));
                }
                ValueWithClass valueWithClass = ValueWithClass.from(value);
                rowValues.add(valueWithClass);
            }

            allRows.add(rowValues);
        }
        return allRows;
    }
}
