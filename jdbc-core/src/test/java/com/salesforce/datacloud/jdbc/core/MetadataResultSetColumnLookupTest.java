/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import lombok.val;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for column lookup optimization in MetadataResultSet.
 * These tests focus on the column lookup logic without requiring a database connection.
 *
 * Note: MetadataResultSet and StreamingResultSet share the same findColumn() and getter
 * method implementations, so these tests also validate the shared column lookup logic.
 */
public class MetadataResultSetColumnLookupTest {

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

    /**
     * Creates a MetadataResultSet with the given columns and data.
     */
    private AvaticaResultSet createMetadataResultSet(List<ColumnMetaData> columns, List<Object> data)
            throws SQLException {
        val signature = new Meta.Signature(
                columns, null, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
        val metaData = new AvaticaResultSetMetaData(null, null, signature);
        return MetadataResultSet.of(null, new QueryState(), signature, metaData, TimeZone.getDefault(), null, data);
    }

    @Test
    public void testFindColumnExactMatch() throws SQLException {
        // Create columns: "Col1" (ordinal 0), "Col2" (ordinal 1), "Col3" (ordinal 2)
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Col1", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "Col2", Types.VARCHAR));
        columns.add(createColumnMetaData(2, "Col3", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of("value1", "value2", "value3"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Test exact matches
            assertThat(resultSet.findColumn("Col1")).isEqualTo(1); // ordinal 0 -> JDBC index 1
            assertThat(resultSet.findColumn("Col2")).isEqualTo(2); // ordinal 1 -> JDBC index 2
            assertThat(resultSet.findColumn("Col3")).isEqualTo(3); // ordinal 2 -> JDBC index 3
        }
    }

    @Test
    public void testFindColumnCaseInsensitiveMatch() throws SQLException {
        // Create columns with different cases: "Aaa" (ordinal 0), "aaa" (ordinal 1), "AaA" (ordinal 2)
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Aaa", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "aaa", Types.VARCHAR));
        columns.add(createColumnMetaData(2, "AaA", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of("value1", "value2", "value3"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Exact matches should work
            assertThat(resultSet.findColumn("Aaa")).isEqualTo(1);
            assertThat(resultSet.findColumn("aaa")).isEqualTo(2);
            assertThat(resultSet.findColumn("AaA")).isEqualTo(3);

            // Case-insensitive matches should fall back to first occurrence (lowest ordinal)
            // "AAA" -> lowercase "aaa" -> matches "Aaa" at ordinal 0 (first processed)
            assertThat(resultSet.findColumn("AAA")).isEqualTo(1);
            assertThat(resultSet.findColumn("aaA")).isEqualTo(1);
            assertThat(resultSet.findColumn("AAa")).isEqualTo(1);
        }
    }

    @Test
    public void testFindColumnNotFound() throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Col1", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of("value1"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Should throw exception for non-existent column
            assertThatThrownBy(() -> resultSet.findColumn("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");
        }
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

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of("value1", "value2"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Column with null label should not be findable by label (only by index)
            assertThatThrownBy(() -> resultSet.findColumn("col1")).isInstanceOf(SQLException.class);

            // But we can still access by index
            assertThat(resultSet.getString(1)).isEqualTo("value1");

            // Column with label should work
            assertThat(resultSet.findColumn("Col2")).isEqualTo(2);
            assertThat(resultSet.getString("Col2")).isEqualTo("value2");
        }
    }

    @Test
    public void testGetterMethodsWithExactMatch() throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "IntCol", Types.INTEGER));
        columns.add(createColumnMetaData(1, "LongCol", Types.BIGINT));
        columns.add(createColumnMetaData(2, "BoolCol", Types.BOOLEAN));
        columns.add(createColumnMetaData(3, "StringCol", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of(42, 123L, true, "test"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Test all getter methods with exact match
            assertThat(resultSet.getInt("IntCol")).isEqualTo(42);
            assertThat(resultSet.getLong("LongCol")).isEqualTo(123L);
            assertThat(resultSet.getBoolean("BoolCol")).isTrue();
            assertThat(resultSet.getString("StringCol")).isEqualTo("test");
        }
    }

    @Test
    public void testGetterMethodsWithCaseInsensitiveMatch() throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "IntCol", Types.INTEGER));
        columns.add(createColumnMetaData(1, "StringCol", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of(42, "test"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Test case-insensitive matches
            assertThat(resultSet.getInt("intcol")).isEqualTo(42);
            assertThat(resultSet.getInt("INTCOL")).isEqualTo(42);
            assertThat(resultSet.getString("stringcol")).isEqualTo("test");
            assertThat(resultSet.getString("STRINGCOL")).isEqualTo("test");
        }
    }

    @Test
    public void testGetterMethodsNotFound() throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "Col1", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of("value1"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // All getter methods should throw exception for non-existent column
            assertThatThrownBy(() -> resultSet.getString("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getInt("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getLong("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getBoolean("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getByte("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getShort("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getFloat("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");

            assertThatThrownBy(() -> resultSet.getDouble("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found");
        }
    }

    @Test
    public void testEmptyColumns() throws SQLException {
        // Test with empty column list
        List<ColumnMetaData> columns = new ArrayList<>();
        List<Object> data = new ArrayList<>();

        try (val resultSet = createMetadataResultSet(columns, data)) {
            // Should throw exception for any column lookup
            assertThatThrownBy(() -> resultSet.findColumn("AnyCol"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'AnyCol' not found");
        }
    }

    @Test
    public void testMultipleColumnsWithSameLowercase() throws SQLException {
        // Test the putIfAbsent behavior - first occurrence (lowest ordinal) should win
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "First", Types.VARCHAR));
        columns.add(createColumnMetaData(1, "FIRST", Types.VARCHAR));
        columns.add(createColumnMetaData(2, "FiRsT", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of("value1", "value2", "value3"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Exact matches should work
            assertThat(resultSet.findColumn("First")).isEqualTo(1);
            assertThat(resultSet.findColumn("FIRST")).isEqualTo(2);
            assertThat(resultSet.findColumn("FiRsT")).isEqualTo(3);

            // Case-insensitive match should return first occurrence (ordinal 0)
            assertThat(resultSet.findColumn("first")).isEqualTo(1); // matches "First" at ordinal 0
            assertThat(resultSet.getString("first")).isEqualTo("value1");
        }
    }

    @Test
    public void testAllGetterTypes() throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(createColumnMetaData(0, "ByteCol", Types.TINYINT));
        columns.add(createColumnMetaData(1, "ShortCol", Types.SMALLINT));
        columns.add(createColumnMetaData(2, "IntCol", Types.INTEGER));
        columns.add(createColumnMetaData(3, "LongCol", Types.BIGINT));
        columns.add(createColumnMetaData(4, "FloatCol", Types.REAL));
        columns.add(createColumnMetaData(5, "DoubleCol", Types.DOUBLE));
        columns.add(createColumnMetaData(6, "BoolCol", Types.BOOLEAN));
        columns.add(createColumnMetaData(7, "StringCol", Types.VARCHAR));

        List<Object> data = new ArrayList<>();
        data.add(ImmutableList.of((byte) 10, (short) 20, 30, 40L, 1.5f, 2.5, true, "test"));

        try (val resultSet = createMetadataResultSet(columns, data)) {
            resultSet.next();

            // Test all getter types
            assertThat(resultSet.getByte("ByteCol")).isEqualTo((byte) 10);
            assertThat(resultSet.getShort("ShortCol")).isEqualTo((short) 20);
            assertThat(resultSet.getInt("IntCol")).isEqualTo(30);
            assertThat(resultSet.getLong("LongCol")).isEqualTo(40L);
            assertThat(resultSet.getFloat("FloatCol")).isEqualTo(1.5f);
            assertThat(resultSet.getDouble("DoubleCol")).isEqualTo(2.5);
            assertThat(resultSet.getBoolean("BoolCol")).isTrue();
            assertThat(resultSet.getString("StringCol")).isEqualTo("test");

            // Test case-insensitive for all types
            assertThat(resultSet.getByte("bytecol")).isEqualTo((byte) 10);
            assertThat(resultSet.getShort("shortcol")).isEqualTo((short) 20);
            assertThat(resultSet.getInt("intcol")).isEqualTo(30);
            assertThat(resultSet.getLong("longcol")).isEqualTo(40L);
            assertThat(resultSet.getFloat("floatcol")).isEqualTo(1.5f);
            assertThat(resultSet.getDouble("doublecol")).isEqualTo(2.5);
            assertThat(resultSet.getBoolean("boolcol")).isTrue();
            assertThat(resultSet.getString("stringcol")).isEqualTo("test");
        }
    }
}
