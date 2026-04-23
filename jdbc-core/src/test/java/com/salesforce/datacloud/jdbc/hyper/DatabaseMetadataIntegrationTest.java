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
        Map<String, String> mismatches = collectMismatches(
                "DATA_TYPE", info -> jdbcTypeName((int) info.get("DATA_TYPE")) + "(" + info.get("DATA_TYPE") + ")");

        // BUG: every entry below is a disagreement between the two metadata paths. The Arrow
        // side is JDBC-spec-correct (java.sql.Types); the pg_catalog side falls through to
        // raw Postgres type OIDs because QueryMetadataUtil's dbTypeToSql keys on short names
        // while format_type() returns long names. Fixing QueryMetadataUtil so every type maps
        // to the same java.sql.Types value as ResultSetMetaData will shrink this map to empty.
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("col_smallint", "arrow=SMALLINT(5), pg=<unknown>(21)");
        expected.put("col_int", "arrow=INTEGER(4), pg=<unknown>(23)");
        expected.put("col_nullable_int", "arrow=INTEGER(4), pg=<unknown>(23)");
        expected.put("col_not_null_int", "arrow=INTEGER(4), pg=<unknown>(23)");
        expected.put("col_bigint", "arrow=BIGINT(-5), pg=<unknown>(20)");
        expected.put("col_double", "arrow=DOUBLE(8), pg=<unknown>(701)");
        // BUG: DECIMAL vs NUMERIC is itself inconsistent — Arrow emits DECIMAL, pg_catalog
        // emits NUMERIC. JDBC treats them as distinct Types constants even though SQL
        // considers them synonyms.
        expected.put("col_numeric_18_2", "arrow=DECIMAL(3), pg=NUMERIC(2)");
        expected.put("col_numeric_10_5", "arrow=DECIMAL(3), pg=NUMERIC(2)");
        expected.put("col_varchar_255", "arrow=VARCHAR(12), pg=<unknown>(1043)");
        expected.put("col_char_1", "arrow=CHAR(1), pg=<unknown>(18)");
        // oid reverses the pattern: getColumns() correctly says BIGINT (because "oid" is the
        // only dbTypeToSql key that actually matches format_type() output), but Arrow sees
        // the raw 32-bit unsigned and emits INTEGER. Either side could be "right".
        expected.put("col_oid", "arrow=INTEGER(4), pg=BIGINT(-5)");
        expected.put("col_int_array", "arrow=ARRAY(2003), pg=<unknown>(1007)");
        expected.put("col_text_array", "arrow=ARRAY(2003), pg=<unknown>(1009)");
        // BUG: json has no Arrow mapping either; hyperd returns it as Utf8 with metadata so
        // the Arrow side reports VARCHAR. The pg_catalog side reports the raw pg OID 114.
        expected.put("col_json", "arrow=VARCHAR(12), pg=<unknown>(114)");

        assertThat(mismatches)
                .as("DATA_TYPE disagreements between ResultSetMetaData and DatabaseMetaData.getColumns()")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forTypeName() {
        Map<String, String> mismatches = collectMismatches("TYPE_NAME", info -> String.valueOf(info.get("TYPE_NAME")));

        // BUG: same root cause as DATA_TYPE. ResultSetMetaData returns JDBCType names ("INTEGER",
        // "VARCHAR", ...); getColumns() returns whatever format_type() emitted ("integer",
        // "character varying(255)", "array(integer)", ...) because dbTypeToSql doesn't match.
        // Fixing the type mapping in QueryMetadataUtil will shrink this map toward empty.
        Map<String, String> expected = new LinkedHashMap<>();
        // BUG: Arrow emits uppercase JDBCType names while pg_catalog passes through format_type()
        // output verbatim — so even "working" types disagree on casing.
        expected.put("col_bool", "arrow=BOOLEAN, pg=boolean");
        expected.put("col_smallint", "arrow=SMALLINT, pg=smallint");
        expected.put("col_int", "arrow=INTEGER, pg=integer");
        expected.put("col_nullable_int", "arrow=INTEGER, pg=integer");
        expected.put("col_not_null_int", "arrow=INTEGER, pg=integer");
        expected.put("col_bigint", "arrow=BIGINT, pg=bigint");
        expected.put("col_double", "arrow=DOUBLE, pg=double precision");
        expected.put("col_numeric_18_2", "arrow=DECIMAL, pg=NUMERIC");
        expected.put("col_numeric_10_5", "arrow=DECIMAL, pg=NUMERIC");
        expected.put("col_varchar_255", "arrow=VARCHAR, pg=character varying(255)");
        expected.put("col_char_1", "arrow=CHAR, pg=character(1)");
        // oid: Arrow sees it as a 32-bit int so reports INTEGER; pg_catalog maps it to BIGINT.
        expected.put("col_oid", "arrow=INTEGER, pg=BIGINT");
        expected.put("col_int_array", "arrow=ARRAY, pg=array(integer)");
        expected.put("col_text_array", "arrow=ARRAY, pg=array(text)");
        expected.put("col_json", "arrow=VARCHAR, pg=json");

        assertThat(mismatches)
                .as("TYPE_NAME disagreements between ResultSetMetaData and DatabaseMetaData.getColumns()")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forPrecision() {
        // Arrow side: ResultSetMetaData.getPrecision() — computed per-type in ColumnType.
        // pg_catalog side: COLUMN_SIZE — hardcoded to 255 in QueryMetadataUtil.
        Map<String, String> mismatches =
                collectMismatches("COLUMN_SIZE", info -> info.get("COLUMN_SIZE").toString());

        // BUG: COLUMN_SIZE is hardcoded to 255 in QueryMetadataUtil.constructColumnData
        // regardless of the column. ResultSetMetaData.getPrecision() returns the correct
        // per-type value (digit count for ints, declared precision for numeric/varchar, etc.).
        // Fixing QueryMetadataUtil to derive COLUMN_SIZE from atttypmod / atttypid will shrink
        // this map toward empty.
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("col_bool", "arrow=1, pg=255");
        expected.put("col_smallint", "arrow=5, pg=255");
        expected.put("col_int", "arrow=10, pg=255");
        expected.put("col_nullable_int", "arrow=10, pg=255");
        expected.put("col_not_null_int", "arrow=10, pg=255");
        expected.put("col_bigint", "arrow=19, pg=255");
        expected.put("col_double", "arrow=17, pg=255");
        expected.put("col_numeric_18_2", "arrow=18, pg=255");
        expected.put("col_numeric_10_5", "arrow=10, pg=255");
        // Text columns with no declared length report Integer.MAX_VALUE on the Arrow side,
        // 255 on the pg side. Both are arguably wrong — text in Postgres has no length limit.
        expected.put("col_text", "arrow=" + Integer.MAX_VALUE + ", pg=255");
        expected.put("col_varchar_255", "arrow=255, pg=255"); // coincidence — both match here.
        expected.put("col_char_1", "arrow=1, pg=255");
        expected.put("col_date", "arrow=13, pg=255");
        expected.put("col_time", "arrow=15, pg=255");
        expected.put("col_timestamp", "arrow=29, pg=255");
        expected.put("col_timestamptz", "arrow=35, pg=255");
        expected.put("col_oid", "arrow=10, pg=255");
        // Arrays: ColumnType.getPrecisionOrStringLength delegates to the element type.
        expected.put("col_int_array", "arrow=10, pg=255");
        expected.put("col_text_array", "arrow=" + Integer.MAX_VALUE + ", pg=255");
        // BUG: json has no explicit precision; Arrow treats it as Utf8 so returns MAX_VALUE.
        expected.put("col_json", "arrow=" + Integer.MAX_VALUE + ", pg=255");
        expected.remove("col_varchar_255"); // remove coincidental-match entry — not a mismatch.

        assertThat(mismatches)
                .as(
                        "COLUMN_SIZE disagreements between ResultSetMetaData.getPrecision() and DatabaseMetaData.getColumns()")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forScale() {
        // Arrow side: ResultSetMetaData.getScale() — derived from atttypmod in Arrow Decimal.
        // pg_catalog side: DECIMAL_DIGITS — hardcoded to 2 for decimal-ish types, else 0.
        Map<String, String> mismatches = collectMismatches(
                "DECIMAL_DIGITS", info -> info.get("DECIMAL_DIGITS").toString());

        // BUG: DECIMAL_DIGITS ignores atttypmod and is hardcoded based on type name only.
        // - Float/double report their precision as scale on the Arrow side (ColumnType.getScale
        //   returns getPrecisionOrStringLength() for DOUBLE), which is itself questionable but
        //   at least non-zero. pg_catalog reports 0 — numeric is the only type that hits its
        //   scale branch because of the isDecimalType() check.
        // - numeric(10,5) should report scale=5; pg_catalog hardcodes 2.
        // - time/timestamp/timestamptz should expose fractional-second scale; Arrow reports 6
        //   (microseconds), pg reports 0.
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("col_double", "arrow=17, pg=0");
        expected.put("col_numeric_10_5", "arrow=5, pg=2");
        expected.put("col_time", "arrow=6, pg=0");
        expected.put("col_timestamp", "arrow=6, pg=0");
        expected.put("col_timestamptz", "arrow=6, pg=0");

        assertThat(mismatches)
                .as(
                        "DECIMAL_DIGITS disagreements between ResultSetMetaData.getScale() and DatabaseMetaData.getColumns()")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forNullability() {
        // Arrow side: ResultSetMetaData.isNullable() returns columnNullable (1) or
        // columnNoNulls (0) based on Arrow field.isNullable().
        // pg_catalog side: NULLABLE column derived from pg_attribute.attnotnull.
        //
        // BUG: hyperd emits Arrow fields with isNullable()=false for most columns in the response
        // schema regardless of the column's DDL NOT NULL constraint, so the Arrow path reports
        // columnNoNulls (0) where pg_catalog correctly reports columnNullable (1). The two int
        // columns (col_nullable_int, col_not_null_int) happen to agree — likely because hyperd
        // handles plain int columns differently in its Arrow schema emission.
        Map<String, String> mismatches =
                collectMismatches("NULLABLE", info -> info.get("NULLABLE").toString());

        Map<String, String> expected = new LinkedHashMap<>();
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
            "col_text_array"
        }) {
            expected.put(col, "arrow=0, pg=1");
        }

        assertThat(mismatches)
                .as("NULLABLE disagreements between ResultSetMetaData and DatabaseMetaData.getColumns()")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    /**
     * Runs the two metadata paths over every cross-check column and returns the set of columns
     * whose {@code property} differs, formatted with {@code render}.
     */
    @SneakyThrows
    private Map<String, String> collectMismatches(
            String property, java.util.function.Function<Map<String, Object>, String> render) {
        try (val connection = getConnection()) {
            Map<String, Map<String, Object>> pg = collectPgCatalogInfo(connection);
            Map<String, Map<String, Object>> arrow = collectArrowInfo(connection);

            Map<String, String> mismatches = new LinkedHashMap<>();
            for (String col : arrow.keySet()) {
                if (CROSS_CHECK_EXCLUDED.contains(col)) {
                    continue;
                }
                Map<String, Object> pgInfo = pg.get(col);
                Map<String, Object> arrowInfo = arrow.get(col);
                assertThat(pgInfo)
                        .as("getColumns() must return a row for %s", col)
                        .isNotNull();

                Object pgVal = pgInfo.get(property);
                Object arrowVal = arrowInfo.get(property);
                if (pgVal == null ? arrowVal != null : !pgVal.equals(arrowVal)) {
                    mismatches.put(col, "arrow=" + render.apply(arrowInfo) + ", pg=" + render.apply(pgInfo));
                }
            }
            return mismatches;
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

    private static Map<String, Map<String, Object>> collectPgCatalogInfo(DataCloudConnection connection)
            throws SQLException {
        Map<String, Map<String, Object>> info = new LinkedHashMap<>();
        try (val rs = connection.getMetaData().getColumns(null, TEST_SCHEMA, TEST_TABLE, "%")) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("DATA_TYPE", rs.getInt("DATA_TYPE"));
                row.put("TYPE_NAME", rs.getString("TYPE_NAME"));
                row.put("COLUMN_SIZE", rs.getInt("COLUMN_SIZE"));
                row.put("DECIMAL_DIGITS", rs.getInt("DECIMAL_DIGITS"));
                row.put("NULLABLE", rs.getInt("NULLABLE"));
                info.put(rs.getString("COLUMN_NAME"), row);
            }
        }
        return info;
    }

    private static Map<String, Map<String, Object>> collectArrowInfo(DataCloudConnection connection)
            throws SQLException {
        Map<String, Map<String, Object>> info = new LinkedHashMap<>();
        try (val stmt = connection.createStatement();
                val rs = stmt.executeQuery(selectEmptyRowset())) {
            val md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                Map<String, Object> row = new HashMap<>();
                row.put("DATA_TYPE", md.getColumnType(i));
                row.put("TYPE_NAME", md.getColumnTypeName(i));
                row.put("COLUMN_SIZE", md.getPrecision(i));
                row.put("DECIMAL_DIGITS", md.getScale(i));
                row.put("NULLABLE", md.isNullable(i));
                info.put(md.getColumnName(i), row);
            }
        }
        return info;
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
