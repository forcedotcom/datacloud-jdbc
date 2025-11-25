/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.SQLException;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
public class MetadataResultSetTest {

    @Test
    @SneakyThrows
    public void testColumnLookupExactMatchPreference() {
        // Test the exact-match-first behavior with case-sensitive columns
        // This verifies that exact matches are preferred over case-insensitive matches
        assertWithConnection(connection -> {
            // Aaa -> 1 (ordinal 0), aaa -> 2 (ordinal 1), AaA -> 3 (ordinal 2)
            String sql = "SELECT 1 as \"Aaa\", 2 as \"aaa\", 3 as \"AaA\" FROM (VALUES(1)) AS t";

            val stmt = connection.prepareStatement(sql);
            val result = stmt.executeQuery();
            assertThat(result.next()).isTrue();

            // Exact matches should work (case-sensitive)
            assertThat(result.getInt("Aaa")).isEqualTo(1); // exact match -> ordinal 0 -> index 1
            assertThat(result.getInt("aaa")).isEqualTo(2); // exact match -> ordinal 1 -> index 2
            assertThat(result.getInt("AaA")).isEqualTo(3); // exact match -> ordinal 2 -> index 3

            // Case-insensitive matches should fall back to first lowercase occurrence
            // With putIfAbsent, the first column processed (lowest ordinal) wins
            // "Aaa" (ordinal 0) is processed first, so it wins for lowercase "aaa"
            // "AAA" -> lowercase "aaa" -> matches "Aaa" at ordinal 0 (first processed)
            assertThat(result.getInt("AAA")).isEqualTo(1); // lowercase match -> "Aaa" -> ordinal 0 -> index 1
            // "aaA" -> lowercase "aaa" -> matches "Aaa" at ordinal 0 (first processed)
            assertThat(result.getInt("aaA")).isEqualTo(1); // lowercase match -> "Aaa" -> ordinal 0 -> index 1

            // Verify exact matches still work after case-insensitive lookups
            assertThat(result.getInt("Aaa")).isEqualTo(1);
            assertThat(result.getInt("aaa")).isEqualTo(2);
            assertThat(result.getInt("AaA")).isEqualTo(3);
        });
    }

    @Test
    @SneakyThrows
    public void testColumnLookupAllGetterMethods() {
        // Test all getter methods with both exact and case-insensitive matches
        assertWithConnection(connection -> {
            String sql = "SELECT "
                    + "123::bigint as \"LongCol\", "
                    + "true as \"BoolCol\", "
                    + "42::smallint as \"ShortCol\", "
                    + "17::smallint as \"ByteCol\", "
                    + "3.14::real as \"FloatCol\", "
                    + "2.718::double precision as \"DoubleCol\" "
                    + "FROM (VALUES(1)) AS t";

            val stmt = connection.prepareStatement(sql);
            val result = stmt.executeQuery();
            assertThat(result.next()).isTrue();

            // Test exact matches for all getter types
            assertThat(result.getLong("LongCol")).isEqualTo(123L);
            assertThat(result.getBoolean("BoolCol")).isTrue();
            assertThat(result.getShort("ShortCol")).isEqualTo((short) 42);
            assertThat(result.getByte("ByteCol")).isEqualTo((byte) 17);
            assertThat(result.getFloat("FloatCol")).isEqualTo(3.14f, offset(0.001f));
            assertThat(result.getDouble("DoubleCol")).isEqualTo(2.718, offset(0.001));

            // Test case-insensitive matches for all getter types
            assertThat(result.getLong("longcol")).isEqualTo(123L);
            assertThat(result.getBoolean("BOOLCOL")).isTrue();
            assertThat(result.getShort("ShortCol")).isEqualTo((short) 42);
            assertThat(result.getByte("bytecol")).isEqualTo((byte) 17);
            assertThat(result.getFloat("FLOATCOL")).isEqualTo(3.14f, offset(0.001f));
            assertThat(result.getDouble("doublecol")).isEqualTo(2.718, offset(0.001));
        });
    }

    @Test
    @SneakyThrows
    public void testColumnLookupNotFound() {
        // Test that column not found throws appropriate exception
        assertWithConnection(connection -> {
            String sql = "SELECT 1 as \"col1\", 2 as \"col2\" FROM (VALUES(1)) AS t";
            val stmt = connection.prepareStatement(sql);
            val result = stmt.executeQuery();
            assertThat(result.next()).isTrue();

            // Test that findColumn throws exception for non-existent column
            assertThatThrownBy(() -> result.findColumn("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            // Test that all getter methods throw exception for non-existent column
            assertThatThrownBy(() -> result.getString("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getInt("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getLong("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getBoolean("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getByte("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getShort("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getFloat("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");

            assertThatThrownBy(() -> result.getDouble("nonexistent"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("column 'nonexistent' not found")
                    .hasFieldOrPropertyWithValue("SQLState", "42703");
        });
    }

    @Test
    @SneakyThrows
    @Disabled
    public void testMetadataResultSetColumnLookup() {
        // Test MetadataResultSet column lookup through DatabaseMetaData
        assertWithConnection(connection -> {
            val metaData = connection.getMetaData();
            // getTables returns a MetadataResultSet
            try (val tables = metaData.getTables(null, null, null, null)) {
                // Test exact match
                if (tables.next()) {
                    // These columns exist in the tables metadata
                    String tableName = tables.getString("TABLE_NAME");
                    assertThat(tableName).isNotNull();

                    // Test case-insensitive match
                    String tableName2 = tables.getString("table_name");
                    assertThat(tableName2).isEqualTo(tableName);

                    // Test other getter methods
                    tables.getString("TABLE_SCHEM");
                    tables.getString("TABLE_CAT");
                }
            }

            // getColumns also returns a MetadataResultSet
            try (val columns = metaData.getColumns(null, null, null, null)) {
                if (columns.next()) {
                    // Test exact and case-insensitive matches
                    columns.getString("COLUMN_NAME");
                    columns.getString("column_name");
                    columns.getInt("DATA_TYPE");
                    columns.getInt("data_type");
                }
            }
        });
    }
}
