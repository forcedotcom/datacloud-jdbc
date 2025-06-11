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
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import com.salesforce.datacloud.reference.ColumnMetadata;
import com.salesforce.datacloud.reference.ReferenceEntry;
import com.salesforce.datacloud.reference.ValueWithClass;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(HyperTestBase.class)
public class JDBCTypesTest {

    /**
     * Loads baseline entries from the baseline.json file.
     *
     * @return Stream of ReferenceEntry objects
     */
    @SneakyThrows
    public static Stream<ReferenceEntry> getBaselineEntries() {
        ObjectMapper objectMapper = new ObjectMapper();

        try (val inputStream = requireNonNull(
                ReferenceEntry.class.getResourceAsStream("/reference.json"),
                "Could not find /reference.json resource")) {
            val referenceEntries = objectMapper.readValue(inputStream, new TypeReference<List<ReferenceEntry>>() {});
            val testableEntries = new ArrayList<ReferenceEntry>();

            for (ReferenceEntry e : referenceEntries) {
                boolean isTestable = true;
                // Patch result metadata expectations
                for (ColumnMetadata c : e.getColumnMetadata()) {
                    // These types don't work in V3
                    if (c.getColumnTypeName().endsWith("regconfig")
                            || c.getColumnTypeName().endsWith("regproc")
                            || c.getColumnTypeName().endsWith("regprocedure")
                            || c.getColumnTypeName().endsWith("regclass")
                            || c.getColumnTypeName().endsWith("regoper")
                            || c.getColumnTypeName().endsWith("regoperator")
                            || c.getColumnTypeName().endsWith("regtype")
                            || c.getColumnTypeName().endsWith("regdictionary")) {
                        isTestable = false;
                    }
                    // This type isn't supported by the driver yet
                    if (c.getColumnTypeName().endsWith("interval")) {
                        isTestable = false;
                    }
                    // These types have a bug in Hyper
                    else if (c.getColumnTypeName().equals("_timestamptz")
                            || c.getColumnTypeName().equals("_timestamp")) {
                        isTestable = false;
                    }
                    // This type behaves differently between Hyper and Postgres and is not worth it to test
                    else if (c.getColumnTypeName().endsWith("oid")) {
                        isTestable = false;
                    }
                    // The JDBC default is to have uppercase type names
                    c.setColumnTypeName(c.getColumnTypeName().toUpperCase());
                    // Change from Postgres specific type names to JDBC type names
                    if ("INT8".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.BIGINT.getName());
                    } else if ("INT2".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.SMALLINT.getName());
                    } else if ("INT4".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.INTEGER.getName());
                    } else if ("FLOAT8".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.DOUBLE.getName());
                    } else if ("FLOAT4".equals(c.getColumnTypeName())) {
                        assert (c.getColumnType() == JDBCType.REAL.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.REAL.getName());
                    } else if ("TEXT".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.VARCHAR.getName());
                    } else if ("BPCHAR".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.CHAR.getName());
                    } else if ("TIMESTAMPTZ".equals(c.getColumnTypeName())) {
                        c.setColumnTypeName(JDBCType.TIMESTAMP_WITH_TIMEZONE.getName());
                    }

                    // Both `Numeric` and `Decimal` can be used, keep using `Decimal` to avoid soft breaking
                    // consumers of older versions
                    if (JDBCType.NUMERIC.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnType(JDBCType.DECIMAL.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.DECIMAL.getName());
                    }
                    // Same reason for BINARY vs VARBINARY
                    if (JDBCType.BINARY.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnType(JDBCType.VARBINARY.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.VARBINARY.getName());
                    }
                    // We use boolean instead of bit
                    if (JDBCType.BIT.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnType(JDBCType.BOOLEAN.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.BOOLEAN.getName());
                    }
                    // We don't have an custom representation for the SQL JSON type
                    if (JDBCType.OTHER.getVendorTypeNumber().equals(c.getColumnType())
                            && "JSON".equals(c.getColumnTypeName())) {
                        c.setColumnType(JDBCType.VARCHAR.getVendorTypeNumber());
                        c.setColumnTypeName(JDBCType.VARCHAR.getName());
                    }
                    // We don't use custom names for array types
                    if (JDBCType.ARRAY.getVendorTypeNumber().equals(c.getColumnType())) {
                        c.setColumnTypeName(JDBCType.ARRAY.getName());
                    }
                }

                // Patch result value expecation
                for (List<ValueWithClass> v : e.getReturnedValues()) {
                    assertEquals(1, v.size(), "The test driver only supports one result value per query");
                    ValueWithClass value = v.get(0);
                    if (e.getQuery().contains("smallint") && (value.getJavaClassName() != null)) {
                        // Avatica doesn't support returning short as Integer (which the standard requires as described
                        // in B-3)
                        value.setJavaClassName(Short.class.getName());
                    } else if ("org.postgresql.util.PGobject".equals(value.getJavaClassName())) {
                        // We return JSON as a String
                        value.setJavaClassName(String.class.getName());
                    } else if (java.sql.Timestamp.class.getName().equals(value.getJavaClassName())) {
                        // We still have several bugs in the driver regarding timestamps and thus we can't compare
                        // against reference values
                        isTestable = false;
                    }
                }
                if (isTestable) {
                    testableEntries.add(e);
                }
            }
            return testableEntries.stream();
        }
    }

    /**
     * Tests DataCloudResultSet metadata and returned values against PostgreSQL baseline expectations.
     * This validates that our JDBC driver produces the same metadata and values as PostgreSQL.
     */
    @ParameterizedTest
    @MethodSource("getBaselineEntries")
    @SneakyThrows
    public void testMetadataAgainstBaseline(ReferenceEntry ReferenceEntry) {
        Properties properties = new Properties();
        properties.setProperty("timezone", "America/Los_Angeles");
        try (DataCloudConnection conn = getHyperQueryConnection(properties)) {

            val stmt = (DataCloudStatement) conn.createStatement();

            try (DataCloudResultSet rs = (DataCloudResultSet) stmt.executeQuery(ReferenceEntry.getQuery())) {
                val metadata = rs.getMetaData();
                val expectedColumns = ReferenceEntry.getColumnMetadata();

                // Validate column count matches
                assertEquals(
                        expectedColumns.size(),
                        metadata.getColumnCount(),
                        "Column count mismatch for query: " + ReferenceEntry.getQuery());

                // Validate each column's metadata
                for (int i = 0; i < expectedColumns.size(); i++) {
                    int columnIndex = i + 1; // JDBC is 1-based
                    ColumnMetadata expected = expectedColumns.get(i);

                    // Extract actual metadata from DataCloudResultSet
                    ColumnMetadata actual = ColumnMetadata.fromResultSetMetaData(metadata, columnIndex);

                    // Compare key metadata fields
                    validateColumnMetadata(expected, actual, ReferenceEntry.getQuery(), columnIndex);
                }

                // Validate returned values and null signaling
                validateReturnedValues(rs, ReferenceEntry);
            } catch (Exception e) {
                System.out.println("Failed to execute query: " + ReferenceEntry.getQuery());
                System.out.println("Error: " + e.getMessage());
                // Skip queries that fail to execute - these might be PostgreSQL-specific
                // In production, we'd want to catalog which queries work vs don't work
                throw (e);
            }
        }
    }

    /**
     * Validates that actual column metadata matches expected baseline metadata.
     *
     * @param expected Expected metadata from PostgreSQL baseline
     * @param actual Actual metadata from DataCloudResultSet
     * @param query The SQL query being tested
     * @param columnIndex The column index (1-based)
     */
    private void validateColumnMetadata(ColumnMetadata expected, ColumnMetadata actual, String query, int columnIndex) {
        String context = String.format("Query: %s, Column: %d", query, columnIndex);
        // Core type information - these should match exactly for compatibility
        assertEquals(expected.getColumnType(), actual.getColumnType(), context + " - Column type mismatch");

        assertEquals(
                expected.getColumnTypeName(), actual.getColumnTypeName(), context + " - Column type name mismatch");

        // Precision and scale
        assertEquals(expected.getPrecision(), actual.getPrecision(), context + " - Precision mismatch");
        assertEquals(expected.getScale(), actual.getScale(), context + " - Scale mismatch");

        // Nullability
        assertEquals(expected.getIsNullable(), actual.getIsNullable(), context + " - Nullability mismatch");

        // Boolean flags - these are important for JDBC behavior
        assertEquals(expected.isAutoIncrement(), actual.isAutoIncrement(), context + " - Auto increment mismatch");
        assertEquals(expected.isCaseSensitive(), actual.isCaseSensitive(), context + " - Case sensitivity mismatch");
        assertEquals(expected.isCurrency(), actual.isCurrency(), context + " - Currency flag mismatch");
        assertEquals(expected.isSigned(), actual.isSigned(), context + " - Signed flag mismatch");

        // Write/search capabilities
        assertEquals(expected.isReadOnly(), actual.isReadOnly(), context + " - Read-only flag mismatch");
        assertEquals(expected.isSearchable(), actual.isSearchable(), context + " - Searchable flag mismatch");

        // Object level check to cover fields that are not individually validated, we reset those values that we
        // explicitly don't want to test
        expected.setColumnName("");
        actual.setColumnName("");
        expected.setColumnLabel("");
        actual.setColumnLabel("");
        expected.setCatalogName("");
        actual.setCatalogName("");
        expected.setSchemaName("");
        actual.setSchemaName("");
        expected.setTableName("");
        actual.setTableName("");
        assertEquals(expected, actual);
    }

    /**
     * Validates that actual returned values match expected baseline values and that null signaling works correctly.
     *
     * @param rs the DataCloudResultSet containing actual values
     * @param referenceEntry the reference entry containing expected values
     * @throws Exception if validation fails
     */
    private void validateReturnedValues(DataCloudResultSet rs, ReferenceEntry referenceEntry) throws Exception {
        val expectedValues = referenceEntry.getReturnedValues();
        Objects.requireNonNull(expectedValues, "Expected values are null");
        val metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        String query = referenceEntry.getQuery();

        // Extract all returned values from the ResultSet
        int rowIndex = 0;
        while (rs.next()) {
            List<ValueWithClass> expectedRow = expectedValues.get(rowIndex);

            for (int col = 1; col <= columnCount; col++) {
                // Get the value using getObject() like the reference generator
                Object value = rs.getObject(col);

                // Test that wasNull() correctly signals null values
                boolean wasNull = rs.wasNull();
                if (value == null && !wasNull) {
                    throw new AssertionError(String.format(
                            "Query '%s', column %d: value is null but wasNull() returned false", query, col));
                }
                if (value != null && wasNull) {
                    throw new AssertionError(String.format(
                            "Query '%s', column %d: value is not null but wasNull() returned true", query, col));
                }

                // Validate each row's values
                ValueWithClass actualValue = ValueWithClass.from(value);
                ValueWithClass expectedValue = expectedRow.get(col - 1);

                // Handle null comparison properly - compare the ValueWithClass objects directly
                if (!Objects.equals(expectedValue, actualValue)) {
                    throw new AssertionError(String.format(
                            "Value mismatch for query '%s', row %d, column %d: expected '%s', got '%s'",
                            query, rowIndex + 1, col, expectedValue, actualValue));
                }
            }

            ++rowIndex;
        }

        // Validate row count matches
        assertEquals(
                expectedValues.size(),
                rowIndex,
                String.format(
                        "Row count mismatch for query '%s': expected %d rows, got %d rows",
                        query, expectedValues.size(), rowIndex));
    }
}
