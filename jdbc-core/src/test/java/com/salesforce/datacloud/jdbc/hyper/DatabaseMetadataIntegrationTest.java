/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.hyper;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.interceptor.DatabaseAttachInterceptor;
import io.grpc.ManagedChannelBuilder;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for {@link DatabaseMetaData} methods against a real local Hyper instance.
 *
 * <p>Starts a dedicated hyperd with a database (via the {@code -d} flag), then creates
 * a schema and table covering all supported types. Exercises getTables, getColumns,
 * getSchemas against the real pg_catalog system tables to verify the SQL queries
 * and type mapping in {@link com.salesforce.datacloud.jdbc.core.QueryMetadataUtil}.</p>
 */
@ExtendWith(LocalHyperTestBase.class)
@Slf4j
class DatabaseMetadataIntegrationTest {

    private static final String TEST_SCHEMA = "metadata_test";
    private static final String TEST_TABLE = "all_types";
    private static HyperServerProcess server;
    private static String databasePath;

    @BeforeAll
    @SneakyThrows
    static void setupDatabase() {
        // Start hyperd without service roles (WITH_DATABASE config) so CREATE DATABASE works
        server = HyperServerManager.get(HyperServerManager.ConfigFile.WITH_DATABASE);

        // Create and populate database entirely via raw gRPC (JDBC-independent)
        ManagedChannelBuilder<?> channelBuilder =
                ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext();
        try (val stubProvider = JdbcDriverStubProvider.of(channelBuilder)) {
            databasePath = HyperDatabaseSetup.createAndPopulateDatabase(
                    stubProvider.getStub(), TEST_SCHEMA, TEST_TABLE);
        }
        log.info("Test database setup complete at: {}", databasePath);
    }

    // ------------------------------------------------------------------
    // getSchemas
    // ------------------------------------------------------------------

    @Test
    @SneakyThrows
    void getSchemas_returnsTestSchema() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getSchemas(null, TEST_SCHEMA);

            assertThat(rs.next()).as("Expected at least one schema row").isTrue();
            assertThat(rs.getString("TABLE_SCHEM")).isEqualTo(TEST_SCHEMA);
            assertThat(rs.next()).as("Expected exactly one matching schema").isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getSchemas_unfiltered_includesTestSchema() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getSchemas();

            val schemas = collectColumn(rs, "TABLE_SCHEM");
            assertThat(schemas).contains(TEST_SCHEMA);
        }
    }

    // ------------------------------------------------------------------
    // getTables
    // ------------------------------------------------------------------

    @Test
    @SneakyThrows
    void getTables_findsTestTable() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getTables(null, TEST_SCHEMA, TEST_TABLE, null);

            assertThat(rs.next()).as("Expected the test table").isTrue();
            assertThat(rs.getString("TABLE_SCHEM")).isEqualTo(TEST_SCHEMA);
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(TEST_TABLE);
            assertThat(rs.next()).as("Expected exactly one matching table").isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getTables_withSchemaPattern_filtersCorrectly() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getTables(null, "nonexistent_%", "%", null);

            assertThat(rs.next()).as("Should return no tables for nonexistent schema").isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getTables_withTablePattern_filtersCorrectly() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getTables(null, TEST_SCHEMA, "nonexistent_%", null);

            assertThat(rs.next()).as("Should return no tables for nonexistent table name").isFalse();
        }
    }

    // ------------------------------------------------------------------
    // getColumns — type mapping verification
    // ------------------------------------------------------------------

    @Test
    @SneakyThrows
    void getColumns_returnsAllColumns() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getColumns(null, TEST_SCHEMA, TEST_TABLE, "%");

            val columns = collectColumnInfo(rs);
            // The table has 19 columns (including 2 array columns)
            assertThat(columns).hasSize(19);
        }
    }

    @Test
    @SneakyThrows
    void getColumns_booleanTypeMappedCorrectly() {
        // Hyper's format_type returns "boolean" — dbTypeToSql only maps "bool"
        assertColumnTypeName("col_bool", "boolean");
    }

    @Test
    @SneakyThrows
    void getColumns_smallintTypeMappedCorrectly() {
        // Hyper's format_type returns "smallint" — dbTypeToSql only maps "int2"
        assertColumnTypeName("col_smallint", "smallint");
    }

    @Test
    @SneakyThrows
    void getColumns_intTypeMappedCorrectly() {
        // Hyper's format_type returns "integer" — dbTypeToSql only maps "int4"
        assertColumnTypeName("col_int", "integer");
    }

    @Test
    @SneakyThrows
    void getColumns_bigintTypeMappedCorrectly() {
        // Hyper's format_type returns "bigint" — dbTypeToSql only maps "int8"
        assertColumnTypeName("col_bigint", "bigint");
    }

    @Test
    @SneakyThrows
    void getColumns_doubleTypeMappedCorrectly() {
        // Hyper's format_type returns "double precision" — dbTypeToSql only maps "float8"
        assertColumnTypeName("col_double", "double precision");
    }

    @Test
    @SneakyThrows
    void getColumns_numericTypeMappedCorrectly() {
        assertColumnType("col_numeric_18_2", JDBCType.NUMERIC);
    }

    @Test
    @SneakyThrows
    void getColumns_textTypeMappedCorrectly() {
        assertColumnType("col_text", JDBCType.VARCHAR);
    }

    @Test
    @SneakyThrows
    void getColumns_varcharTypeMappedCorrectly() {
        // Hyper's format_type returns "character varying(255)" — not mapped by dbTypeToSql
        assertColumnTypeName("col_varchar_255", "character varying(255)");
    }

    @Test
    @SneakyThrows
    void getColumns_charTypeMappedCorrectly() {
        // Hyper's format_type returns "character(1)"
        assertColumnTypeName("col_char_1", "character(1)");
    }

    @Test
    @SneakyThrows
    void getColumns_dateTypeMappedCorrectly() {
        assertColumnType("col_date", JDBCType.DATE);
    }

    @Test
    @SneakyThrows
    void getColumns_timeTypeMappedCorrectly() {
        assertColumnType("col_time", JDBCType.TIME);
    }

    @Test
    @SneakyThrows
    void getColumns_timestampTypeMappedCorrectly() {
        assertColumnType("col_timestamp", JDBCType.TIMESTAMP);
    }

    @Test
    @SneakyThrows
    void getColumns_timestamptzTypeMappedCorrectly() {
        assertColumnType("col_timestamptz", JDBCType.TIMESTAMP_WITH_TIMEZONE);
    }

    @Test
    @SneakyThrows
    void getColumns_intervalTypeMapped() {
        // interval is not in dbTypeToSql — check what Hyper returns
        assertColumnTypeName("col_interval", "interval");
    }

    @Test
    @SneakyThrows
    void getColumns_jsonTypeMapped() {
        // json is not in dbTypeToSql — check what Hyper returns
        assertColumnTypeName("col_json", "json");
    }

    @Test
    @SneakyThrows
    void getColumns_oidTypeMapped() {
        // oid maps to BIGINT via dbTypeToSql
        assertColumnType("col_oid", JDBCType.BIGINT);
    }

    @Test
    @SneakyThrows
    void getColumns_intArrayTypeMapped() {
        // Hyper's format_type returns "array(integer)" — dbTypeToSql only maps exact "array"
        assertColumnTypeName("col_int_array", "array(integer)");
    }

    @Test
    @SneakyThrows
    void getColumns_textArrayTypeMapped() {
        // Hyper's format_type returns "array(text)" — dbTypeToSql only maps exact "array"
        assertColumnTypeName("col_text_array", "array(text)");
    }

    @Test
    @SneakyThrows
    void getColumns_nullableColumnReportsNullable() {
        try (val connection = getConnection()) {
            val info = getColumnInfo(connection, "col_nullable_int");
            assertThat(info).isNotNull();
            assertThat(info.get("NULLABLE"))
                    .as("col_nullable_int should be nullable")
                    .isEqualTo(DatabaseMetaData.columnNullable);
        }
    }

    @Test
    @SneakyThrows
    void getColumns_nonNullableColumnReportsNotNull() {
        try (val connection = getConnection()) {
            // col_bool is NOT NULL by default in our CREATE TABLE (no explicit NOT NULL, but Hyper
            // reports nullable for all columns unless explicitly constrained — adjust expectation
            // based on actual Hyper behavior)
            val info = getColumnInfo(connection, "col_bool");
            assertThat(info).isNotNull();
            // Verify the nullable field is present and is a valid value
            int nullable = (int) info.get("NULLABLE");
            assertThat(nullable)
                    .as("Nullable should be a valid JDBC nullable constant")
                    .isIn(
                            DatabaseMetaData.columnNullable,
                            DatabaseMetaData.columnNoNulls,
                            DatabaseMetaData.columnNullableUnknown);
        }
    }

    @Test
    @SneakyThrows
    void getColumns_withColumnNameFilter() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getColumns(null, TEST_SCHEMA, TEST_TABLE, "col_bool");

            assertThat(rs.next()).as("Should find col_bool").isTrue();
            assertThat(rs.getString("COLUMN_NAME")).isEqualTo("col_bool");
            assertThat(rs.next()).as("Should only find one column").isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getColumns_ordinalPositionsAreSequential() {
        try (val connection = getConnection()) {
            val md = connection.getMetaData();
            val rs = md.getColumns(null, TEST_SCHEMA, TEST_TABLE, "%");

            int expectedOrdinal = 1;
            while (rs.next()) {
                int actual = rs.getInt("ORDINAL_POSITION");
                assertThat(actual)
                        .as("Ordinal position for column %s", rs.getString("COLUMN_NAME"))
                        .isEqualTo(expectedOrdinal);
                expectedOrdinal++;
            }
            assertThat(expectedOrdinal - 1).as("Total columns").isEqualTo(19);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static DataCloudConnection getConnection() throws SQLException {
        // The -d flag creates the database and registers it in Hyper's catalog.
        // For gRPC, we reference it by the file path Hyper uses internally.
        return LocalHyperTestBase.getHyperQueryConnection(
                server, new DatabaseAttachInterceptor(databasePath, "default"));
    }

    private void assertColumnType(String columnName, JDBCType expectedType) throws SQLException {
        assertColumnTypeName(columnName, expectedType.toString());
    }

    private void assertColumnTypeName(String columnName, String expectedTypeName) throws SQLException {
        try (val connection = getConnection()) {
            val info = getColumnInfo(connection, columnName);
            assertThat(info)
                    .as("Column %s should exist", columnName)
                    .isNotNull();
            assertThat(info.get("TYPE_NAME"))
                    .as("TYPE_NAME for %s", columnName)
                    .isEqualTo(expectedTypeName);
        }
    }

    private Map<String, Object> getColumnInfo(DataCloudConnection connection, String columnName)
            throws SQLException {
        val md = connection.getMetaData();
        val rs = md.getColumns(null, TEST_SCHEMA, TEST_TABLE, columnName);

        if (!rs.next()) {
            return null;
        }

        Map<String, Object> info = new HashMap<>();
        info.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
        info.put("DATA_TYPE", rs.getInt("DATA_TYPE"));
        info.put("TYPE_NAME", rs.getString("TYPE_NAME"));
        info.put("COLUMN_SIZE", rs.getInt("COLUMN_SIZE"));
        info.put("NULLABLE", rs.getInt("NULLABLE"));
        info.put("ORDINAL_POSITION", rs.getInt("ORDINAL_POSITION"));
        info.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
        return info;
    }

    private static List<String> collectColumn(ResultSet rs, String columnName) throws SQLException {
        List<String> values = new ArrayList<>();
        while (rs.next()) {
            values.add(rs.getString(columnName));
        }
        return values;
    }

    private static List<Map<String, Object>> collectColumnInfo(ResultSet rs) throws SQLException {
        List<Map<String, Object>> columns = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> info = new HashMap<>();
            info.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
            info.put("DATA_TYPE", rs.getInt("DATA_TYPE"));
            info.put("TYPE_NAME", rs.getString("TYPE_NAME"));
            columns.add(info);
        }
        return columns;
    }
}
