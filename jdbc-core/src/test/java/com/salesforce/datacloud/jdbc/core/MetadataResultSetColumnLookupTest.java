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
 * Unit tests for MetadataResultSet getter methods with column names.
 * These tests verify that all getter methods (getString, getInt, getLong, getDouble, etc.)
 * properly use the ColumnNameResolver through findColumn().
 *
 * <p>For comprehensive tests of the column lookup logic itself, see {@link ColumnNameResolverTest}.
 * For integration tests with a real database, see {@link MetadataResultSetTest}.
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

            // Test all getter types with exact match
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
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getInt("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getLong("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getBoolean("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getByte("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getShort("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getFloat("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> resultSet.getDouble("NonExistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'NonExistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");
        }
    }
}
