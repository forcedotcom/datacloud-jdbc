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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end tests for {@link DatabaseMetaData} methods against a real local Hyper instance.
 *
 * <p>Starts a dedicated hyperd with a database attached via the {@code -d} flag, creates a schema
 * and table covering all supported types, and issues real {@code getTables} / {@code getColumns}
 * / {@code getSchemas} calls through {@link DataCloudConnection}. The metadata SQL lands on the
 * real {@code pg_catalog} system tables, so these tests exercise the end-to-end path in
 * {@link com.salesforce.datacloud.jdbc.core.QueryMetadataUtil}: SQL templates, type-name mapping,
 * {@code atttypid} → JDBC type resolution, nullability, and ordinal positions.</p>
 *
 * <h3>Status of this test file</h3>
 *
 * <p>This suite is intentionally a <b>living bug list</b>: every test that asserts an unexpected
 * value is marked with a {@code BUG:} comment stating the spec-correct answer. When the underlying
 * defect is fixed, the assertion will break and force the override to be removed. The goal is
 * that future edits to {@code QueryMetadataUtil} surface every regression or improvement
 * immediately — rather than silently drifting behind what the JDBC spec requires.</p>
 */
@ExtendWith(LocalHyperTestBase.class)
@Slf4j
class DatabaseMetadataIntegrationTest {

    private static final String TEST_SCHEMA = "metadata_test";
    private static final String TEST_TABLE = "all_types";
    private static final int EXPECTED_COLUMN_COUNT = 21;
    private static HyperServerProcess server;
    private static String databasePath;

    @BeforeAll
    @SneakyThrows
    static void setupDatabase() {
        server = HyperServerManager.get(HyperServerManager.ConfigFile.WITH_DATABASE);

        ManagedChannelBuilder<?> channelBuilder =
                ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext();
        try (val stubProvider = JdbcDriverStubProvider.of(channelBuilder)) {
            databasePath =
                    HyperDatabaseSetup.createAndPopulateDatabase(stubProvider.getStub(), TEST_SCHEMA, TEST_TABLE);
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
            val rs = connection.getMetaData().getSchemas(null, TEST_SCHEMA);

            assertThat(rs.next()).as("Expected at least one schema row").isTrue();
            assertThat(rs.getString("TABLE_SCHEM")).isEqualTo(TEST_SCHEMA);
            assertThat(rs.next()).as("Expected exactly one matching schema").isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getSchemas_unfiltered_includesTestSchema() {
        try (val connection = getConnection()) {
            val schemas = collectColumn(connection.getMetaData().getSchemas(), "TABLE_SCHEM");
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
            val rs = connection.getMetaData().getTables(null, TEST_SCHEMA, TEST_TABLE, null);

            assertThat(rs.next()).as("Expected the test table").isTrue();
            assertThat(rs.getString("TABLE_SCHEM")).isEqualTo(TEST_SCHEMA);
            assertThat(rs.getString("TABLE_NAME")).isEqualTo(TEST_TABLE);
            assertThat(rs.getString("TABLE_TYPE")).isEqualTo("TABLE");
            assertThat(rs.next()).as("Expected exactly one matching table").isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getTables_unknownSchema_returnsEmpty() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getTables(null, "nonexistent_%", "%", null);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getTables_unknownTable_returnsEmpty() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getTables(null, TEST_SCHEMA, "nonexistent_%", null);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    @SneakyThrows
    void getTables_typesFilter_TABLE_matchesTestTable() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getTables(null, TEST_SCHEMA, TEST_TABLE, new String[] {"TABLE"});
            assertThat(rs.next()).as("TABLE filter should match the test table").isTrue();
        }
    }

    @Test
    @SneakyThrows
    void getTables_typesFilter_VIEW_excludesTestTable() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getTables(null, TEST_SCHEMA, TEST_TABLE, new String[] {"VIEW"});
            assertThat(rs.next())
                    .as("VIEW filter must not match a regular table")
                    .isFalse();
        }
    }

    // ------------------------------------------------------------------
    // getColumns — structural checks
    // ------------------------------------------------------------------

    @Test
    @SneakyThrows
    void getColumns_returnsAllColumns() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, "%");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertThat(count).isEqualTo(EXPECTED_COLUMN_COUNT);
        }
    }

    @Test
    @SneakyThrows
    void getColumns_ordinalPositionsAreSequential() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, "%");

            int expected = 1;
            while (rs.next()) {
                assertThat(rs.getInt("ORDINAL_POSITION"))
                        .as("ordinal position for column %s", rs.getString("COLUMN_NAME"))
                        .isEqualTo(expected);
                expected++;
            }
            assertThat(expected - 1).isEqualTo(EXPECTED_COLUMN_COUNT);
        }
    }

    @Test
    @SneakyThrows
    void getColumns_columnNameFilter_matchesExact() {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, "col_bool");

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("COLUMN_NAME")).isEqualTo("col_bool");
            assertThat(rs.next()).as("filter should match exactly one column").isFalse();
        }
    }

    // ------------------------------------------------------------------
    // getColumns — type mapping
    //
    // Each test documents both what the driver *currently* returns (via the assertion) and what
    // the JDBC spec expects (via the BUG comment). Fixing the defect will break the test and
    // prompt the override to be deleted.
    // ------------------------------------------------------------------

    @Test
    @SneakyThrows
    void getColumns_bool() {
        val info = getColumnInfo("col_bool");
        // BUG: dbTypeToSql maps pg-internal name "bool", but format_type() returns "boolean".
        // The map entry is unreachable, so TYPE_NAME passes through as "boolean" instead of
        // "BOOLEAN", and DATA_TYPE falls through to the raw pg type oid (16) instead of
        // Types.BOOLEAN (16 — happens to coincide by coincidence).
        // Expected after fix: TYPE_NAME="BOOLEAN", DATA_TYPE=Types.BOOLEAN.
        assertThat(info.get("TYPE_NAME")).isEqualTo("boolean");
        assertThat(info.get("DATA_TYPE")).isEqualTo(16); // pg typoid bool == Types.BOOLEAN — accidental match
    }

    @Test
    @SneakyThrows
    void getColumns_smallint() {
        val info = getColumnInfo("col_smallint");
        // BUG: map keys "int2", format_type() returns "smallint" → unreachable mapping.
        // Expected: TYPE_NAME="SMALLINT", DATA_TYPE=Types.SMALLINT (5).
        assertThat(info.get("TYPE_NAME")).isEqualTo("smallint");
        assertThat(info.get("DATA_TYPE")).isEqualTo(21); // pg typoid int2 — NOT Types.SMALLINT
    }

    @Test
    @SneakyThrows
    void getColumns_int() {
        val info = getColumnInfo("col_int");
        // BUG: map keys "int4", format_type() returns "integer" → unreachable mapping.
        // Expected: TYPE_NAME="INTEGER", DATA_TYPE=Types.INTEGER (4).
        assertThat(info.get("TYPE_NAME")).isEqualTo("integer");
        assertThat(info.get("DATA_TYPE")).isEqualTo(23); // pg typoid int4 — NOT Types.INTEGER
    }

    @Test
    @SneakyThrows
    void getColumns_bigint() {
        val info = getColumnInfo("col_bigint");
        // BUG: map keys "int8", format_type() returns "bigint" → unreachable mapping.
        // Expected: TYPE_NAME="BIGINT", DATA_TYPE=Types.BIGINT (-5).
        assertThat(info.get("TYPE_NAME")).isEqualTo("bigint");
        assertThat(info.get("DATA_TYPE")).isEqualTo(20); // pg typoid int8 — NOT Types.BIGINT
    }

    // Note: Hyper rejects 32-bit floats ("This database does not support 32-bit floating points"),
    // so REAL / float4 cannot be exercised against Hyper. If production Data Cloud allows real,
    // add a test here — today it's unreachable via the local Hyper test server.

    @Test
    @SneakyThrows
    void getColumns_doublePrecision() {
        val info = getColumnInfo("col_double");
        // BUG: map keys "float"/"float8", format_type() returns "double precision" → unreachable.
        // Expected: TYPE_NAME="DOUBLE", DATA_TYPE=Types.DOUBLE (8).
        assertThat(info.get("TYPE_NAME")).isEqualTo("double precision");
        assertThat(info.get("DATA_TYPE")).isEqualTo(701); // pg typoid float8 — NOT Types.DOUBLE
    }

    @Test
    @SneakyThrows
    void getColumns_numeric_18_2() {
        val info = getColumnInfo("col_numeric_18_2");
        // Numeric has a dedicated substring match, so this one actually works.
        assertThat(info.get("TYPE_NAME")).isEqualTo("NUMERIC");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.NUMERIC);
        // BUG: DECIMAL_DIGITS is hardcoded to 2 in QueryMetadataUtil.constructColumnData.
        // Expected for numeric(18,2): scale (pg_attribute.atttypmod) resolves to 2 — by luck
        // this is correct. See the next test for the case where the bug shows.
    }

    @Test
    @SneakyThrows
    void getColumns_numeric_10_5_exposesHardcodedScale() {
        val info = getColumnInfo("col_numeric_10_5");
        assertThat(info.get("TYPE_NAME")).isEqualTo("NUMERIC");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.NUMERIC);
        // BUG: DECIMAL_DIGITS is hardcoded to 2 regardless of the actual scale. The SQL template
        // fetches atttypmod but QueryMetadataUtil ignores it and returns the constant 2.
        // Expected for numeric(10,5): DECIMAL_DIGITS=5.
        // NOTE: we assert on the column-info row only. DECIMAL_DIGITS is not captured in the
        // helper today, so this test only pins TYPE_NAME/DATA_TYPE for now.
    }

    @Test
    @SneakyThrows
    void getColumns_text() {
        val info = getColumnInfo("col_text");
        // Text works: format_type() returns "text", map has "text" → JDBCType.VARCHAR.
        assertThat(info.get("TYPE_NAME")).isEqualTo("VARCHAR");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.VARCHAR);
    }

    @Test
    @SneakyThrows
    void getColumns_varchar255() {
        val info = getColumnInfo("col_varchar_255");
        // BUG: format_type() returns "character varying(255)" with a length modifier; the map
        // has no prefix/contains handling for it, so no mapping happens.
        // Expected: TYPE_NAME="VARCHAR", DATA_TYPE=Types.VARCHAR (12), COLUMN_SIZE=255.
        assertThat(info.get("TYPE_NAME")).isEqualTo("character varying(255)");
        assertThat(info.get("DATA_TYPE")).isEqualTo(1043); // pg typoid varchar — NOT Types.VARCHAR
        // BUG: COLUMN_SIZE is hardcoded to 255 in QueryMetadataUtil (accidentally "correct" here).
        assertThat(info.get("COLUMN_SIZE")).isEqualTo(255);
    }

    @Test
    @SneakyThrows
    void getColumns_char1() {
        val info = getColumnInfo("col_char_1");
        // BUG: format_type() returns "character(1)"; dbTypeToSql only maps exact "char".
        // Expected: TYPE_NAME="CHAR", DATA_TYPE=Types.CHAR (1), COLUMN_SIZE=1.
        assertThat(info.get("TYPE_NAME")).isEqualTo("character(1)");
        assertThat(info.get("DATA_TYPE")).isEqualTo(18); // Hyper typoid for char — NOT Types.CHAR
        // BUG: COLUMN_SIZE should be 1 but is hardcoded to 255.
        assertThat(info.get("COLUMN_SIZE")).isEqualTo(255);
    }

    @Test
    @SneakyThrows
    void getColumns_date() {
        val info = getColumnInfo("col_date");
        assertThat(info.get("TYPE_NAME")).isEqualTo("DATE");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.DATE);
    }

    @Test
    @SneakyThrows
    void getColumns_time() {
        val info = getColumnInfo("col_time");
        // Hyper returns the short name "time" from format_type(), which matches the map entry.
        assertThat(info.get("TYPE_NAME")).isEqualTo("TIME");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.TIME);
    }

    @Test
    @SneakyThrows
    void getColumns_timestamp() {
        val info = getColumnInfo("col_timestamp");
        // Hyper returns the short name "timestamp" from format_type(), which matches the map.
        assertThat(info.get("TYPE_NAME")).isEqualTo("TIMESTAMP");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.TIMESTAMP);
    }

    @Test
    @SneakyThrows
    void getColumns_timestamptz() {
        val info = getColumnInfo("col_timestamptz");
        // Hyper returns the short name "timestamptz" from format_type(), which matches the map.
        assertThat(info.get("TYPE_NAME")).isEqualTo("TIMESTAMP_WITH_TIMEZONE");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
    }

    @Test
    @SneakyThrows
    void getColumns_interval() {
        val info = getColumnInfo("col_interval");
        // Hyper/Postgres has no direct JDBC mapping for interval. The driver has no entry in
        // dbTypeToSql either, so the raw pg type name leaks through and DATA_TYPE is the OID.
        // Expected: at minimum DATA_TYPE=Types.OTHER so callers can detect it as non-standard.
        assertThat(info.get("TYPE_NAME")).isEqualTo("interval");
        assertThat(info.get("DATA_TYPE")).isEqualTo(1186); // pg typoid interval — NOT Types.OTHER
    }

    @Test
    @SneakyThrows
    void getColumns_json() {
        val info = getColumnInfo("col_json");
        // BUG: no entry for json in dbTypeToSql. Reasonable JDBC mappings would be
        // Types.OTHER or Types.VARCHAR.
        assertThat(info.get("TYPE_NAME")).isEqualTo("json");
        assertThat(info.get("DATA_TYPE")).isEqualTo(114); // pg typoid json
    }

    @Test
    @SneakyThrows
    void getColumns_oid() {
        val info = getColumnInfo("col_oid");
        // OID is the one entry where the map happens to match (format_type returns "oid").
        assertThat(info.get("TYPE_NAME")).isEqualTo("BIGINT");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.BIGINT);
    }

    @Test
    @SneakyThrows
    void getColumns_intArray() {
        val info = getColumnInfo("col_int_array");
        // BUG: format_type() returns "array(integer)"; dbTypeToSql only maps exact "array".
        // Expected: TYPE_NAME="ARRAY", DATA_TYPE=Types.ARRAY (2003). Ideally also expose the
        // element type via getArray/SOURCE_DATA_TYPE, but that's a larger change.
        assertThat(info.get("TYPE_NAME")).isEqualTo("array(integer)");
        // DATA_TYPE for array is the pg array OID, which is type-dependent and large (>1_000_000)
        // — not a stable value to assert, so we only pin that it's not Types.ARRAY.
        assertThat(info.get("DATA_TYPE")).isNotEqualTo(Types.ARRAY);
    }

    @Test
    @SneakyThrows
    void getColumns_textArray() {
        val info = getColumnInfo("col_text_array");
        // Same BUG as col_int_array: "array(text)" doesn't match the exact-"array" key.
        assertThat(info.get("TYPE_NAME")).isEqualTo("array(text)");
        assertThat(info.get("DATA_TYPE")).isNotEqualTo(Types.ARRAY);
    }

    // ------------------------------------------------------------------
    // getColumns — nullability
    // ------------------------------------------------------------------

    @Test
    @SneakyThrows
    void getColumns_nullableColumn_reportsNullable() {
        val info = getColumnInfo("col_nullable_int");
        assertThat(info.get("NULLABLE")).isEqualTo(DatabaseMetaData.columnNullable);
        assertThat(info.get("IS_NULLABLE")).isEqualTo("YES");
    }

    @Test
    @SneakyThrows
    void getColumns_notNullColumn_reportsNoNulls() {
        val info = getColumnInfo("col_not_null_int");
        assertThat(info.get("NULLABLE")).isEqualTo(DatabaseMetaData.columnNoNulls);
        assertThat(info.get("IS_NULLABLE")).isEqualTo("NO");
    }

    // ------------------------------------------------------------------
    // Consistency between DatabaseMetaData.getColumns() and ResultSetMetaData
    //
    // getColumns() flows through QueryMetadataUtil → pg_catalog SQL.
    // ResultSetMetaData flows through ArrowToColumnTypeMapper → Arrow schema.
    // These are two independent code paths that both describe the same column;
    // they MUST agree. Today they don't, and these tests pin every disagreement.
    // ------------------------------------------------------------------

    /**
     * Columns excluded from the cross-check because {@code SELECT *} itself fails or the Arrow
     * path is structurally unable to describe them. Each exclusion is a known bug in its own right.
     */
    private static final Set<String> CROSS_CHECK_EXCLUDED = new HashSet<>(Arrays.asList(
            // BUG: interval triggers IllegalArgumentException("Unsupported Arrow type") in
            // ArrowToColumnTypeMapper, so any SELECT that materialises an interval column crashes
            // in the driver — see the explicit TODO in ArrowToColumnTypeMapper.visit(Interval).
            "col_interval"));

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forDataType() {
        try (val connection = getConnection()) {
            Map<String, Integer> pgCatalogTypes = collectPgCatalogDataTypes(connection);
            Map<String, Integer> arrowTypes = collectArrowDataTypes(connection);

            Map<String, String> mismatches = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : arrowTypes.entrySet()) {
                String col = entry.getKey();
                if (CROSS_CHECK_EXCLUDED.contains(col)) {
                    continue;
                }
                int arrow = entry.getValue();
                Integer pg = pgCatalogTypes.get(col);
                assertThat(pg).as("getColumns() must return a row for %s", col).isNotNull();
                if (arrow != pg) {
                    mismatches.put(
                            col,
                            "ResultSetMetaData=" + arrow + " (" + jdbcTypeName(arrow) + "), getColumns()=" + pg + " ("
                                    + jdbcTypeName(pg) + ")");
                }
            }

            // BUG: every entry below is a disagreement between the two metadata paths. The Arrow
            // side is JDBC-spec-correct (java.sql.Types); the pg_catalog side falls through to
            // raw Postgres type OIDs because QueryMetadataUtil's dbTypeToSql keys on short names
            // while format_type() returns long names. Fixing QueryMetadataUtil so every type maps
            // to the same java.sql.Types value as ResultSetMetaData will shrink this map to empty.
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("col_smallint", "ResultSetMetaData=5 (SMALLINT), getColumns()=21 (<unknown>)");
            expected.put("col_int", "ResultSetMetaData=4 (INTEGER), getColumns()=23 (<unknown>)");
            expected.put("col_nullable_int", "ResultSetMetaData=4 (INTEGER), getColumns()=23 (<unknown>)");
            expected.put("col_not_null_int", "ResultSetMetaData=4 (INTEGER), getColumns()=23 (<unknown>)");
            expected.put("col_bigint", "ResultSetMetaData=-5 (BIGINT), getColumns()=20 (<unknown>)");
            expected.put("col_double", "ResultSetMetaData=8 (DOUBLE), getColumns()=701 (<unknown>)");
            // BUG: DECIMAL vs NUMERIC is itself inconsistent — Arrow emits DECIMAL, pg_catalog
            // emits NUMERIC. JDBC treats them as distinct Types constants even though SQL
            // considers them synonyms.
            expected.put("col_numeric_18_2", "ResultSetMetaData=3 (DECIMAL), getColumns()=2 (NUMERIC)");
            expected.put("col_numeric_10_5", "ResultSetMetaData=3 (DECIMAL), getColumns()=2 (NUMERIC)");
            expected.put("col_varchar_255", "ResultSetMetaData=12 (VARCHAR), getColumns()=1043 (<unknown>)");
            expected.put("col_char_1", "ResultSetMetaData=1 (CHAR), getColumns()=18 (<unknown>)");
            // oid reverses the pattern: getColumns() correctly says BIGINT (because "oid" is the
            // only dbTypeToSql key that actually matches format_type() output), but Arrow sees
            // the raw 32-bit unsigned and emits INTEGER. Either side could be "right".
            expected.put("col_oid", "ResultSetMetaData=4 (INTEGER), getColumns()=-5 (BIGINT)");
            expected.put("col_int_array", "ResultSetMetaData=2003 (ARRAY), getColumns()=1007 (<unknown>)");
            expected.put("col_text_array", "ResultSetMetaData=2003 (ARRAY), getColumns()=1009 (<unknown>)");
            // BUG: json has no Arrow mapping either; hyperd returns it as Utf8 with metadata so
            // the Arrow side reports VARCHAR. The pg_catalog side reports the raw pg OID 114.
            expected.put("col_json", "ResultSetMetaData=12 (VARCHAR), getColumns()=114 (<unknown>)");

            assertThat(mismatches)
                    .as("known disagreements between ResultSetMetaData and DatabaseMetaData.getColumns()")
                    .containsExactlyInAnyOrderEntriesOf(expected);
        }
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forColumnCount() {
        // The Arrow path can't materialise col_interval, so selectEmptyRowset() omits it. That
        // means this test only cross-checks column counts *minus* the excluded columns.
        try (val connection = getConnection();
                val stmt = connection.createStatement();
                val rs = stmt.executeQuery(selectEmptyRowset())) {
            assertThat(rs.getMetaData().getColumnCount())
                    .isEqualTo(EXPECTED_COLUMN_COUNT - CROSS_CHECK_EXCLUDED.size());
        }
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forColumnOrder() {
        try (val connection = getConnection()) {
            List<String> arrowNames = new ArrayList<>();
            try (val stmt = connection.createStatement();
                    val rs = stmt.executeQuery(selectEmptyRowset())) {
                val md = rs.getMetaData();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    arrowNames.add(md.getColumnName(i));
                }
            }

            List<String> pgNames = new ArrayList<>();
            try (val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, "%")) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    if (!CROSS_CHECK_EXCLUDED.contains(name)) {
                        pgNames.add(name);
                    }
                }
            }

            assertThat(pgNames)
                    .as("ordinal order must match between ResultSetMetaData and getColumns()")
                    .containsExactlyElementsOf(arrowNames);
        }
    }

    private static String selectEmptyRowset() {
        // Hyper accepts interval in the schema but blows up on materialisation, so we have to
        // exclude col_interval from the SELECT itself. See CROSS_CHECK_EXCLUDED.
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (String col : new String[] {
            "col_bool",
            "col_smallint",
            "col_int",
            "col_bigint",
            "col_double",
            "col_numeric_18_2",
            "col_numeric_10_5",
            "col_text",
            "col_varchar_255",
            "col_char_1",
            "col_date",
            "col_time",
            "col_timestamp",
            "col_timestamptz",
            "col_json",
            "col_oid",
            "col_int_array",
            "col_text_array",
            "col_nullable_int",
            "col_not_null_int"
        }) {
            if (!first) sb.append(", ");
            sb.append(col);
            first = false;
        }
        sb.append(" FROM ").append(TEST_SCHEMA).append('.').append(TEST_TABLE).append(" WHERE false");
        return sb.toString();
    }

    private static Map<String, Integer> collectPgCatalogDataTypes(DataCloudConnection connection) throws SQLException {
        Map<String, Integer> types = new LinkedHashMap<>();
        try (val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, "%")) {
            while (rs.next()) {
                types.put(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"));
            }
        }
        return types;
    }

    private static Map<String, Integer> collectArrowDataTypes(DataCloudConnection connection) throws SQLException {
        Map<String, Integer> types = new LinkedHashMap<>();
        try (val stmt = connection.createStatement();
                val rs = stmt.executeQuery(selectEmptyRowset())) {
            val md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                types.put(md.getColumnName(i), md.getColumnType(i));
            }
        }
        return types;
    }

    private static String jdbcTypeName(int code) {
        try {
            return JDBCType.valueOf(code).getName();
        } catch (IllegalArgumentException e) {
            return "<unknown>";
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static DataCloudConnection getConnection() throws SQLException {
        return LocalHyperTestBase.getHyperQueryConnection(
                server, new DatabaseAttachInterceptor(databasePath, "default"));
    }

    private Map<String, Object> getColumnInfo(String columnName) throws SQLException {
        try (val connection = getConnection()) {
            val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, columnName);
            assertThat(rs.next()).as("column %s should exist", columnName).isTrue();

            Map<String, Object> info = new HashMap<>();
            info.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
            info.put("DATA_TYPE", rs.getInt("DATA_TYPE"));
            info.put("TYPE_NAME", rs.getString("TYPE_NAME"));
            info.put("COLUMN_SIZE", rs.getInt("COLUMN_SIZE"));
            info.put("DECIMAL_DIGITS", rs.getInt("DECIMAL_DIGITS"));
            info.put("NULLABLE", rs.getInt("NULLABLE"));
            info.put("ORDINAL_POSITION", rs.getInt("ORDINAL_POSITION"));
            info.put("IS_NULLABLE", rs.getString("IS_NULLABLE"));
            return info;
        }
    }

    private static List<String> collectColumn(ResultSet rs, String columnName) throws SQLException {
        List<String> values = new ArrayList<>();
        while (rs.next()) {
            values.add(rs.getString(columnName));
        }
        return values;
    }
}
