/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.salesforce.datacloud.jdbc.core.metadata.ColumnMetadata;
import com.salesforce.datacloud.jdbc.core.metadata.ColumnType;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ColumnNameResolver.
 * These tests focus on the column name resolution logic without requiring a database connection
 * or ResultSet boilerplate.
 */
public class ColumnNameResolverTest {

    private ColumnMetadata createColumn(String name) {
        return new ColumnMetadata(name, new ColumnType(JDBCType.VARCHAR, true), "VARCHAR");
    }

    @Test
    public void testFindColumnExactMatch() throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(createColumn("Col1"));
        columns.add(createColumn("Col2"));
        columns.add(createColumn("Col3"));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        assertThat(resolver.findColumn("Col1")).isEqualTo(1);
        assertThat(resolver.findColumn("Col2")).isEqualTo(2);
        assertThat(resolver.findColumn("Col3")).isEqualTo(3);
    }

    @Test
    public void testFindColumnCaseInsensitiveMatch() throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(createColumn("Aaa"));
        columns.add(createColumn("aaa"));
        columns.add(createColumn("AaA"));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Exact matches should work
        assertThat(resolver.findColumn("Aaa")).isEqualTo(1);
        assertThat(resolver.findColumn("aaa")).isEqualTo(2);
        assertThat(resolver.findColumn("AaA")).isEqualTo(3);

        // Case-insensitive matches should fall back to first occurrence (lowest ordinal)
        assertThat(resolver.findColumn("AAA")).isEqualTo(1);
        assertThat(resolver.findColumn("aaA")).isEqualTo(1);
        assertThat(resolver.findColumn("AAa")).isEqualTo(1);
    }

    @Test
    public void testFindColumnNotFound() {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(createColumn("Col1"));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        assertThatThrownBy(() -> resolver.findColumn("NonExistent"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("column 'NonExistent' not found")
                .hasFieldOrPropertyWithValue("SQLState", "42703");
    }

    @Test
    public void testFindColumnWithNullLabels() throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata(null, new ColumnType(JDBCType.VARCHAR, true), "VARCHAR"));
        columns.add(createColumn("Col2"));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Column with null label should not be findable
        assertThatThrownBy(() -> resolver.findColumn("col1"))
                .isInstanceOf(SQLException.class)
                .hasFieldOrPropertyWithValue("SQLState", "42703");

        // Column with label should work
        assertThat(resolver.findColumn("Col2")).isEqualTo(2);
    }

    @Test
    public void testEmptyColumns() {
        List<ColumnMetadata> columns = new ArrayList<>();

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        assertThatThrownBy(() -> resolver.findColumn("AnyCol"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("column 'AnyCol' not found")
                .hasFieldOrPropertyWithValue("SQLState", "42703");
    }

    @Test
    public void testMultipleColumnsWithSameLowercase() throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(createColumn("First"));
        columns.add(createColumn("FIRST"));
        columns.add(createColumn("FiRsT"));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // Exact matches should work
        assertThat(resolver.findColumn("First")).isEqualTo(1);
        assertThat(resolver.findColumn("FIRST")).isEqualTo(2);
        assertThat(resolver.findColumn("FiRsT")).isEqualTo(3);

        // Case-insensitive match should return first occurrence
        assertThat(resolver.findColumn("first")).isEqualTo(1);
    }

    @Test
    public void testDuplicateColumnNames() throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(createColumn("Duplicate"));
        columns.add(new ColumnMetadata("Other", new ColumnType(JDBCType.INTEGER, true), "INTEGER"));
        columns.add(createColumn("Duplicate"));

        ColumnNameResolver resolver = new ColumnNameResolver(columns);

        // First occurrence should win
        assertThat(resolver.findColumn("Duplicate")).isEqualTo(1);
        assertThat(resolver.findColumn("Other")).isEqualTo(2);
    }
}
