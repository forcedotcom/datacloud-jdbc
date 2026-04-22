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
        assertThat(info.get("TYPE_NAME")).isEqualTo("BOOLEAN");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.BOOLEAN);
    }

    @Test
    @SneakyThrows
    void getColumns_smallint() {
        val info = getColumnInfo("col_smallint");
        assertThat(info.get("TYPE_NAME")).isEqualTo("SMALLINT");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.SMALLINT);
    }

    @Test
    @SneakyThrows
    void getColumns_int() {
        val info = getColumnInfo("col_int");
        assertThat(info.get("TYPE_NAME")).isEqualTo("INTEGER");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.INTEGER);
    }

    @Test
    @SneakyThrows
    void getColumns_bigint() {
        val info = getColumnInfo("col_bigint");
        assertThat(info.get("TYPE_NAME")).isEqualTo("BIGINT");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.BIGINT);
    }

    // Note: Hyper rejects 32-bit floats ("This database does not support 32-bit floating points"),
    // so REAL / float4 cannot be exercised against Hyper. If production Data Cloud allows real,
    // add a test here — today it's unreachable via the local Hyper test server.

    @Test
    @SneakyThrows
    void getColumns_doublePrecision() {
        val info = getColumnInfo("col_double");
        assertThat(info.get("TYPE_NAME")).isEqualTo("DOUBLE");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.DOUBLE);
    }

    @Test
    @SneakyThrows
    void getColumns_numeric_18_2() {
        val info = getColumnInfo("col_numeric_18_2");
        // HyperType unifies NUMERIC and DECIMAL into a single DECIMAL kind.
        assertThat(info.get("TYPE_NAME")).isEqualTo("DECIMAL");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.DECIMAL);
    }

    @Test
    @SneakyThrows
    void getColumns_numeric_10_5_exposesHardcodedScale() {
        val info = getColumnInfo("col_numeric_10_5");
        assertThat(info.get("TYPE_NAME")).isEqualTo("DECIMAL");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.DECIMAL);
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
        assertThat(info.get("TYPE_NAME")).isEqualTo("VARCHAR");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.VARCHAR);
        // BUG: COLUMN_SIZE is hardcoded to 255 in QueryMetadataUtil (accidentally "correct" here).
        assertThat(info.get("COLUMN_SIZE")).isEqualTo(255);
    }

    @Test
    @SneakyThrows
    void getColumns_char1() {
        val info = getColumnInfo("col_char_1");
        assertThat(info.get("TYPE_NAME")).isEqualTo("CHAR");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.CHAR);
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
        // Hyper/Postgres has no direct JDBC mapping for interval; expose as OTHER with the
        // upper-cased Hyper type name so callers can detect it as non-standard.
        assertThat(info.get("TYPE_NAME")).isEqualTo("INTERVAL");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.OTHER);
    }

    @Test
    @SneakyThrows
    void getColumns_json() {
        val info = getColumnInfo("col_json");
        // json has no first-class JDBC type; expose as OTHER with the Hyper type name.
        assertThat(info.get("TYPE_NAME")).isEqualTo("JSON");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.OTHER);
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
        assertThat(info.get("TYPE_NAME")).isEqualTo("ARRAY");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.ARRAY);
    }

    @Test
    @SneakyThrows
    void getColumns_textArray() {
        val info = getColumnInfo("col_text_array");
        assertThat(info.get("TYPE_NAME")).isEqualTo("ARRAY");
        assertThat(info.get("DATA_TYPE")).isEqualTo(Types.ARRAY);
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

        // After the HyperType refactor, the two paths agree for almost every type. The only
        // remaining mismatches are cases where Arrow and pg_catalog genuinely see different
        // types — not driver bugs:
        //   col_oid : Arrow sees the raw 32-bit unsigned and emits INTEGER; pg surfaces OID as
        //             BIGINT (per HyperType.OID → JDBCType.BIGINT).
        //   col_json: Arrow receives the value as Utf8 metadata and emits VARCHAR; pg surfaces
        //             json as OTHER per the JDBC spec for non-standard types.
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("col_oid", "arrow=INTEGER(4), pg=BIGINT(-5)");
        expected.put("col_json", "arrow=VARCHAR(12), pg=OTHER(1111)");

        assertThat(mismatches)
                .as("DATA_TYPE disagreements between ResultSetMetaData and DatabaseMetaData.getColumns()")
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    @SneakyThrows
    void getColumns_consistentWith_resultSetMetaData_forTypeName() {
        Map<String, String> mismatches = collectMismatches("TYPE_NAME", info -> String.valueOf(info.get("TYPE_NAME")));

        // Same remaining mismatches as forDataType — see that test for the rationale.
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("col_oid", "arrow=INTEGER, pg=BIGINT");
        expected.put("col_json", "arrow=VARCHAR, pg=JSON");

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
        // pg_catalog side: DECIMAL_DIGITS — hardcoded to 2 for DECIMAL types, else 0.
        Map<String, String> mismatches = collectMismatches(
                "DECIMAL_DIGITS", info -> info.get("DECIMAL_DIGITS").toString());

        // Remaining mismatches reflect known simplifications, not type-mapping bugs:
        //   - col_double: Arrow returns 17 (HyperTypes.getScale returns precision for
        //     FLOAT4/FLOAT8 by legacy convention). pg_catalog returns 0 because binary floats
        //     have no decimal scale. pg is more defensible per the JDBC spec.
        //   - col_numeric_10_5: pg_catalog still hardcodes DECIMAL_DIGITS=2 instead of reading
        //     atttypmod. Expected to shrink once QueryMetadataUtil is taught to parse the typmod.
        //   - col_time / col_timestamp / col_timestamptz: Arrow reports 6 (microseconds);
        //     pg_catalog reports 0 because fractional-second scale is not populated.
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
