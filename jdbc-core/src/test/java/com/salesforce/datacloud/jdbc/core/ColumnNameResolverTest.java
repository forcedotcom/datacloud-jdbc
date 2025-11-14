/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import lombok.val;
import org.apache.calcite.avatica.ColumnMetaData;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ColumnNameResolver.
 * These tests focus on the column name resolution logic without requiring a database connection
 * or ResultSet boilerplate.
 */
public class ColumnNameResolverTest {

    /**
     * Creates a ColumnMetaData instance for testing.
     */
    private ColumnMetaData createColumnMetaData(int ordinal, String label, int sqlType) {
        val avaticaType = ColumnMetaData.scalar(sqlType, label, ColumnMetaData.Rep.PRIMITIVE_BOOLEAN);
        return new ColumnMetaData(
                ordinal,
                false, // autoIncrement
                true, // caseSensitive
                true, // searchable
                false, // currency
                1, // nullable
                true, // signed
                10, // displaySize
                label, // label
                label, // columnName
                null, // schemaName
                0, // precision
                0, // scale
                null, // tableName
                null, // catalogName
                avaticaType,
                true, // readOnly
                false, // writable
                false, // definitelyWritable
                "java.lang.String"); // className
    }

    @Test
    public void testFindColumnExactMatch() throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Col1", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "Col2", Types.VARCHAR));
        columns.add(createColumnMetaData(2, "Col3", Types.VARCHAR));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Test exact matches
        assertThat(resolver.findColumn("Col1")).isEqualTo(1); // ordinal 0 -> JDBC index 1
        assertThat(resolver.findColumn("Col2")).isEqualTo(2); // ordinal 1 -> JDBC index 2
        assertThat(resolver.findColumn("Col3")).isEqualTo(3); // ordinal 2 -> JDBC index 3
    }

    @Test
    public void testFindColumnCaseInsensitiveMatch() throws SQLException {
        // Create columns with different cases: "Aaa" (ordinal 0), "aaa" (ordinal 1), "AaA" (ordinal 2)
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Aaa", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "aaa", Types.VARCHAR));
        columns.add(createColumnMetaData(2, "AaA", Types.VARCHAR));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Exact matches should work
        assertThat(resolver.findColumn("Aaa")).isEqualTo(1);
        assertThat(resolver.findColumn("aaa")).isEqualTo(2);
        assertThat(resolver.findColumn("AaA")).isEqualTo(3);

        // Case-insensitive matches should fall back to first occurrence (lowest ordinal)
        // "AAA" -> lowercase "aaa" -> matches "Aaa" at ordinal 0 (first processed)
        assertThat(resolver.findColumn("AAA")).isEqualTo(1);
        assertThat(resolver.findColumn("aaA")).isEqualTo(1);
        assertThat(resolver.findColumn("AAa")).isEqualTo(1);
    }

    @Test
    public void testFindColumnNotFound() {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Col1", Types.VARCHAR));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Should throw exception for non-existent column
        assertThatThrownBy(() -> resolver.findColumn("NonExistent"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("column 'NonExistent' not found")
                .hasFieldOrPropertyWithValue("SQLState", "42703");
    }

    @Test
    public void testFindColumnWithNullLabels() throws SQLException {
        // Create columns where some have null labels
        List<ColumnMetaData> columns = new ArrayList<>();
        // Column with null label (should be skipped in maps)
        val columnWithNullLabel = new ColumnMetaData(
                0,
                false,
                true,
                true,
                false,
                1,
                true,
                10,
                null, // null label
                "col1",
                null,
                0,
                0,
                null,
                null,
                ColumnMetaData.scalar(Types.VARCHAR, "col1", ColumnMetaData.Rep.PRIMITIVE_BOOLEAN),
                true,
                false,
                false,
                "java.lang.String");
        columns.add(columnWithNullLabel);
        columns.add(createColumnMetaData(1, "Col2", Types.VARCHAR));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Column with null label should not be findable by label
        assertThatThrownBy(() -> resolver.findColumn("col1"))
                .isInstanceOf(SQLException.class)
                .hasFieldOrPropertyWithValue("SQLState", "42703");

        // Column with label should work
        assertThat(resolver.findColumn("Col2")).isEqualTo(2);
    }

    @Test
    public void testEmptyColumns() {
        // Test with empty column list
        List<ColumnMetaData> columns = new ArrayList<>();

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Should throw exception for any column lookup
        assertThatThrownBy(() -> resolver.findColumn("AnyCol"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("column 'AnyCol' not found")
                .hasFieldOrPropertyWithValue("SQLState", "42703");
    }

    @Test
    public void testMultipleColumnsWithSameLowercase() throws SQLException {
        // Test the putIfAbsent behavior - first occurrence (lowest ordinal) should win
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "First", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "FIRST", Types.VARCHAR));
        columns.add(createColumnMetaData(2, "FiRsT", Types.VARCHAR));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Exact matches should work
        assertThat(resolver.findColumn("First")).isEqualTo(1);
        assertThat(resolver.findColumn("FIRST")).isEqualTo(2);
        assertThat(resolver.findColumn("FiRsT")).isEqualTo(3);

        // Case-insensitive match should return first occurrence (ordinal 0)
        assertThat(resolver.findColumn("first")).isEqualTo(1); // matches "First" at ordinal 0
    }

    @Test
    public void testDuplicateColumnNames() throws SQLException {
        // Test that duplicate column names return the first column (lowest ordinal)
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Duplicate", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "Other", Types.INTEGER));
        columns.add(createColumnMetaData(2, "Duplicate", Types.VARCHAR));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // When looking up "Duplicate", should return the first occurrence (ordinal 0)
        assertThat(resolver.findColumn("Duplicate")).isEqualTo(1); // ordinal 0 -> JDBC index 1

        // Other column should still work
        assertThat(resolver.findColumn("Other")).isEqualTo(2); // ordinal 1 -> JDBC index 2
    }
}
