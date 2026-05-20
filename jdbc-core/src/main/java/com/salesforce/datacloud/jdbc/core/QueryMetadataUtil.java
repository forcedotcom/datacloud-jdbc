/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.google.common.collect.Maps.immutableEntry;
import static com.salesforce.datacloud.jdbc.config.QueryResources.getColumnsQueryText;
import static com.salesforce.datacloud.jdbc.config.QueryResources.getSchemasQueryText;
import static com.salesforce.datacloud.jdbc.config.QueryResources.getTablesQueryText;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.metadata.MetadataResultSets;
import com.salesforce.datacloud.jdbc.core.types.HyperTypes;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import com.salesforce.datacloud.jdbc.protocol.data.HyperType;
import com.salesforce.datacloud.jdbc.protocol.data.PgCatalogTypeParser;
import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
final class QueryMetadataUtil {
    private static final int TABLE_CATALOG_INDEX = 0;
    private static final int TABLE_SCHEMA_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;
    private static final int COLUMN_NAME_INDEX = 3;
    private static final int DATA_TYPE_INDEX = 4;
    private static final int TYPE_NAME_INDEX = 5;
    private static final int COLUMN_SIZE_INDEX = 6;
    private static final int BUFFER_LENGTH_INDEX = 7;
    private static final int DECIMAL_DIGITS_INDEX = 8;
    private static final int NUM_PREC_RADIX_INDEX = 9;
    private static final int NULLABLE_INDEX = 10;
    private static final int DESCRIPTION_INDEX = 11;
    private static final int COLUMN_DEFAULT_INDEX = 12;
    private static final int SQL_DATA_TYPE_INDEX = 13;
    private static final int SQL_DATE_TIME_SUB_INDEX = 14;
    private static final int CHAR_OCTET_LENGTH_INDEX = 15;
    private static final int ORDINAL_POSITION_INDEX = 16;
    private static final int IS_NULLABLE_INDEX = 17;
    private static final int SCOPE_CATALOG_INDEX = 18;
    private static final int SCOPE_SCHEMA_INDEX = 19;
    private static final int SCOPE_TABLE_INDEX = 20;
    private static final int SOURCE_DATA_TYPE_INDEX = 21;
    private static final int AUTO_INCREMENT_INDEX = 22;
    private static final int GENERATED_COLUMN_INDEX = 23;

    private QueryMetadataUtil() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static ResultSet createTableResultSet(
            String schemaPattern, String tableNamePattern, String[] types, Connection connection) throws SQLException {
        final List<Object> data;

        try (val statement = connection.createStatement()) {
            val tablesQuery = getTablesQuery(schemaPattern, tableNamePattern, types);
            val resultSet = statement.executeQuery(tablesQuery);
            data = constructTableData(resultSet);
        }

        return getMetadataResultSet(MetadataSchemas.TABLES, data);
    }

    static ResultSet getMetadataResultSet(List<ColumnMetadata> columns, List<Object> data) throws SQLException {
        return MetadataResultSets.ofRawRows(columns, data);
    }

    private static List<Object> constructTableData(ResultSet resultSet) throws SQLException {
        List<Object> data = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> rowData = Arrays.asList(
                    resultSet.getString("TABLE_CAT"),
                    resultSet.getString("TABLE_SCHEM"),
                    resultSet.getString("TABLE_NAME"),
                    "TABLE",
                    resultSet.getString("REMARKS"),
                    resultSet.getString("TYPE_CAT"),
                    resultSet.getString("TYPE_SCHEM"),
                    resultSet.getString("TYPE_NAME"),
                    resultSet.getString("SELF_REFERENCING_COL_NAME"),
                    resultSet.getString("REF_GENERATION"));
            data.add(rowData);
        }
        return data;
    }

    private static String getTablesQuery(String schemaPattern, String tableNamePattern, String[] types) {
        String tablesQuery = getTablesQueryText();

        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            tablesQuery += " AND n.nspname LIKE " + quoteStringLiteral(schemaPattern);
        }

        if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
            tablesQuery += " AND c.relname LIKE " + quoteStringLiteral(tableNamePattern);
        }
        if (types != null && types.length > 0) {
            tablesQuery += " AND (false ";
            StringBuilder orclause = new StringBuilder();
            for (String type : types) {
                Map<String, String> clauses = tableTypeClauses.get(type);
                if (clauses != null) {
                    String clause = clauses.get("SCHEMAS");
                    orclause.append(" OR ( ").append(clause).append(" ) ");
                }
            }
            tablesQuery += orclause.toString() + ") ";
        }

        tablesQuery += " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ";

        return tablesQuery;
    }

    public static ResultSet createColumnResultSet(
            String schemaPattern, String tableNamePattern, String columnNamePattern, Connection connection)
            throws SQLException {

        final List<Object> data;

        try (val statement = connection.createStatement()) {
            val getColumnsQuery = getColumnsQueryInner(schemaPattern, tableNamePattern, columnNamePattern);
            val resultSet = statement.executeQuery(getColumnsQuery);
            data = constructColumnData(resultSet);
        }

        return getMetadataResultSet(MetadataSchemas.COLUMNS, data);
    }

    private static String getColumnsQueryInner(
            String schemaPattern, String tableNamePattern, String columnNamePattern) {
        String columnsQuery = getColumnsQueryText();

        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            columnsQuery += " AND n.nspname LIKE " + quoteStringLiteral(schemaPattern);
        }
        if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
            columnsQuery += " AND c.relname LIKE " + quoteStringLiteral(tableNamePattern);
        }
        if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
            columnsQuery += " AND attname LIKE " + quoteStringLiteral(columnNamePattern);
        }
        columnsQuery += " ORDER BY nspname, c.relname, attnum ";

        return columnsQuery;
    }

    private static List<Object> constructColumnData(ResultSet resultSet) throws SQLException {
        List<Object> data = new ArrayList<>();
        while (resultSet.next()) {
            Object[] rowData = new Object[24];

            rowData[TABLE_CATALOG_INDEX] = null;
            rowData[TABLE_SCHEMA_INDEX] = resultSet.getString("nspname");
            rowData[TABLE_NAME_INDEX] = resultSet.getString("relname");
            rowData[COLUMN_NAME_INDEX] = resultSet.getString("attname");

            boolean notNull = resultSet.getBoolean("attnotnull");
            String datatype = resultSet.getString("datatype");
            HyperType hyperType;
            try {
                hyperType = PgCatalogTypeParser.parse(datatype, !notNull);
            } catch (IllegalArgumentException ex) {
                // Hyper may surface system-catalog types the driver does not model
                // (e.g. aclitem, array(aclitem), tsvector). Callers that scan
                // getColumns(null, null, null, null) will walk into pg_catalog and hit
                // these — failing the whole metadata query is too aggressive, so we fall
                // back to an UNKNOWN HyperType that preserves the raw name for debugging
                // but surfaces as java.sql.Types.OTHER over JDBC.
                hyperType = HyperType.unknown(datatype, !notNull);
            }

            rowData[DATA_TYPE_INDEX] = HyperTypes.toJdbcTypeCode(hyperType);
            rowData[TYPE_NAME_INDEX] = HyperTypes.toJdbcTypeName(hyperType);

            // COLUMN_SIZE: per JDBC spec this is the declared precision (digits for numerics,
            // characters for strings). HyperTypes.getPrecision already implements this for
            // every HyperTypeKind, so we just forward.
            int columnSize = HyperTypes.getPrecision(hyperType);
            rowData[COLUMN_SIZE_INDEX] = columnSize;

            // DECIMAL_DIGITS: only meaningful for fixed-scale decimals; for those the scale
            // comes from the HyperType that PgCatalogTypeParser extracted from
            // format_type(atttypmod) (e.g. "numeric(10,5)" → scale=5).
            rowData[DECIMAL_DIGITS_INDEX] = HyperTypes.needsDecimalDigits(hyperType) ? hyperType.getScale() : 0;
            rowData[NUM_PREC_RADIX_INDEX] = 10;
            rowData[NULLABLE_INDEX] = notNull ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable;
            rowData[DESCRIPTION_INDEX] = resultSet.getString("description");
            rowData[COLUMN_DEFAULT_INDEX] = resultSet.getString("adsrc");
            rowData[SQL_DATA_TYPE_INDEX] = null;
            rowData[SQL_DATE_TIME_SUB_INDEX] = null;
            rowData[CHAR_OCTET_LENGTH_INDEX] = HyperTypes.needsCharOctetLength(hyperType) ? columnSize : null;
            rowData[ORDINAL_POSITION_INDEX] = resultSet.getInt("attnum");
            rowData[IS_NULLABLE_INDEX] = notNull ? "NO" : "YES";
            rowData[SCOPE_CATALOG_INDEX] = null;
            rowData[SCOPE_SCHEMA_INDEX] = null;
            rowData[SCOPE_TABLE_INDEX] = null;
            rowData[SOURCE_DATA_TYPE_INDEX] = null;

            String identity = resultSet.getString("attidentity");
            String defval = resultSet.getString("adsrc");
            rowData[AUTO_INCREMENT_INDEX] =
                    (defval != null && defval.contains("nextval(")) || identity != null ? "YES" : "NO";

            String generated = resultSet.getString("attgenerated");
            rowData[GENERATED_COLUMN_INDEX] = generated != null ? "YES" : "NO";

            data.add(Arrays.asList(rowData));
        }
        return data;
    }

    public static ResultSet createSchemaResultSet(String schemaPattern, Connection connection) throws SQLException {

        final List<Object> data;

        try (val statement = connection.createStatement()) {
            val schemasQuery = getSchemasQuery(schemaPattern);
            val resultSet = statement.executeQuery(schemasQuery);
            data = constructSchemaData(resultSet);
        }

        return getMetadataResultSet(MetadataSchemas.SCHEMAS, data);
    }

    private static String getSchemasQuery(String schemaPattern) {
        String schemasQuery = getSchemasQueryText();
        if (StringCompatibility.isNotEmpty(schemaPattern)) {
            schemasQuery += " AND nspname LIKE " + quoteStringLiteral(schemaPattern);
        }
        return schemasQuery;
    }

    private static List<Object> constructSchemaData(ResultSet resultSet) throws SQLException {
        List<Object> data = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> rowData =
                    Arrays.asList(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_CATALOG"));
            data.add(rowData);
        }
        return data;
    }

    public static ResultSet createTableTypesResultSet() throws SQLException {
        List<Object> data = constructTableTypesData();
        return getMetadataResultSet(MetadataSchemas.TABLE_TYPES, data);
    }

    private static List<Object> constructTableTypesData() {
        List<Object> data = new ArrayList<>();

        for (Map.Entry<String, Map<String, String>> entry : tableTypeClauses.entrySet()) {
            List<Object> rowData = Arrays.asList(entry.getKey());
            data.add(rowData);
        }
        return data;
    }

    static List<Object> getLakehouse(ThrowingJdbcSupplier<String> lakehouseSupplier) throws SQLException {
        if (lakehouseSupplier == null) {
            return ImmutableList.of();
        }

        val lakehouse = lakehouseSupplier.get();

        if (Strings.isNullOrEmpty(lakehouse)) {
            return ImmutableList.of();
        }

        return ImmutableList.of(ImmutableList.of(lakehouse));
    }

    public static ResultSet createCatalogsResultSet(ThrowingJdbcSupplier<String> lakehouseSupplier)
            throws SQLException {
        val data = getLakehouse(lakehouseSupplier);
        return getMetadataResultSet(MetadataSchemas.CATALOGS, data);
    }

    private static final Map<String, Map<String, String>> tableTypeClauses = ImmutableMap.ofEntries(
            immutableEntry(
                    "TABLE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
                            "NOSCHEMAS",
                            "c.relkind = 'r' AND c.relname !~ '^pg_'")),
            immutableEntry(
                    "PARTITIONED TABLE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
                            "NOSCHEMAS",
                            "c.relkind = 'p' AND c.relname !~ '^pg_'")),
            immutableEntry(
                    "VIEW",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'",
                            "NOSCHEMAS",
                            "c.relkind = 'v' AND c.relname !~ '^pg_'")),
            immutableEntry(
                    "INDEX",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
                            "NOSCHEMAS",
                            "c.relkind = 'i' AND c.relname !~ '^pg_'")),
            immutableEntry(
                    "PARTITIONED INDEX",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'I' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
                            "NOSCHEMAS",
                            "c.relkind = 'I' AND c.relname !~ '^pg_'")),
            immutableEntry("SEQUENCE", ImmutableMap.of("SCHEMAS", "c.relkind = 'S'", "NOSCHEMAS", "c.relkind = 'S'")),
            immutableEntry(
                    "TYPE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
                            "NOSCHEMAS",
                            "c.relkind = 'c' AND c.relname !~ '^pg_'")),
            immutableEntry(
                    "SYSTEM TABLE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')",
                            "NOSCHEMAS",
                            "c.relkind = 'r' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'")),
            immutableEntry(
                    "SYSTEM TOAST TABLE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'r' AND n.nspname = 'pg_toast'",
                            "NOSCHEMAS",
                            "c.relkind = 'r' AND c.relname ~ '^pg_toast_'")),
            immutableEntry(
                    "SYSTEM TOAST INDEX",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'i' AND n.nspname = 'pg_toast'",
                            "NOSCHEMAS",
                            "c.relkind = 'i' AND c.relname ~ '^pg_toast_'")),
            immutableEntry(
                    "SYSTEM VIEW",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ",
                            "NOSCHEMAS",
                            "c.relkind = 'v' AND c.relname ~ '^pg_'")),
            immutableEntry(
                    "SYSTEM INDEX",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ",
                            "NOSCHEMAS",
                            "c.relkind = 'v' AND c.relname ~ '^pg_' AND c.relname !~ '^pg_toast_' AND c.relname !~ '^pg_temp_'")),
            immutableEntry(
                    "TEMPORARY TABLE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind IN ('r','p') AND n.nspname ~ '^pg_temp_' ",
                            "NOSCHEMAS",
                            "c.relkind IN ('r','p') AND c.relname ~ '^pg_temp_' ")),
            immutableEntry(
                    "TEMPORARY INDEX",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'i' AND n.nspname ~ '^pg_temp_' ",
                            "NOSCHEMAS",
                            "c.relkind = 'i' AND c.relname ~ '^pg_temp_' ")),
            immutableEntry(
                    "TEMPORARY VIEW",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'v' AND n.nspname ~ '^pg_temp_' ",
                            "NOSCHEMAS",
                            "c.relkind = 'v' AND c.relname ~ '^pg_temp_' ")),
            immutableEntry(
                    "TEMPORARY SEQUENCE",
                    ImmutableMap.of(
                            "SCHEMAS",
                            "c.relkind = 'S' AND n.nspname ~ '^pg_temp_' ",
                            "NOSCHEMAS",
                            "c.relkind = 'S' AND c.relname ~ '^pg_temp_' ")),
            immutableEntry(
                    "FOREIGN TABLE", ImmutableMap.of("SCHEMAS", "c.relkind = 'f'", "NOSCHEMAS", "c.relkind = 'f'")),
            immutableEntry(
                    "MATERIALIZED VIEW",
                    ImmutableMap.of("SCHEMAS", "c.relkind = 'm'", "NOSCHEMAS", "c.relkind = 'm'")));

    public static String quoteStringLiteral(String v) {
        StringBuilder result = new StringBuilder();

        result.ensureCapacity(v.length() + 8);

        result.append("E'");

        boolean escaped = false;

        for (int i = 0; i < v.length(); i++) {
            char ch = v.charAt(i);
            switch (ch) {
                case '\'':
                    result.append("''");
                    break;
                case '\\':
                    result.append("\\\\");
                    escaped = true;
                    break;
                case '\n':
                    result.append("\\n");
                    escaped = true;
                    break;
                case '\r':
                    result.append("\\r");
                    escaped = true;
                    break;
                case '\t':
                    result.append("\\t");
                    escaped = true;
                    break;
                case '\b':
                    result.append("\\b");
                    escaped = true;
                    break;
                case '\f':
                    result.append("\\f");
                    escaped = true;
                    break;
                default:
                    if (ch < ' ') {
                        result.append('\\').append(String.format("%03o", (int) ch));
                        escaped = true;
                    } else {
                        result.append(ch);
                    }
            }
        }

        if (!escaped) {
            result.deleteCharAt(0);
        }

        return result.append('\'').toString();
    }
}
