/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core;

import static com.google.common.collect.Maps.immutableEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.auth.OAuthToken;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import com.salesforce.datacloud.jdbc.config.QueryResources;
import com.salesforce.datacloud.jdbc.core.model.DataspaceResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import com.salesforce.datacloud.jdbc.util.ArrowUtils;
import com.salesforce.datacloud.jdbc.util.Constants;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.SqlType;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@UtilityClass
class QueryMetadataUtil {
    static final int NUM_TABLE_METADATA_COLUMNS = 10;
    static final int NUM_COLUMN_METADATA_COLUMNS = 24;
    static final int NUM_SCHEMA_METADATA_COLUMNS = 2;
    static final int NUM_TABLE_TYPES_METADATA_COLUMNS = 1;
    static final int NUM_CATALOG_METADATA_COLUMNS = 1;
    private static final String SOQL_ENDPOINT_SUFFIX = "services/data/v61.0/query/";
    private static final String SOQL_QUERY_PARAM_KEY = "q";

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

    private static final Map<String, String> dbTypeToSql = ImmutableMap.ofEntries(
            immutableEntry("int2", SqlType.SMALLINT.toString()),
            immutableEntry("int4", SqlType.INTEGER.toString()),
            immutableEntry("oid", SqlType.BIGINT.toString()),
            immutableEntry("int8", SqlType.BIGINT.toString()),
            immutableEntry("float", SqlType.DOUBLE.toString()),
            immutableEntry("float4", SqlType.REAL.toString()),
            immutableEntry("float8", SqlType.DOUBLE.toString()),
            immutableEntry("bool", SqlType.BOOLEAN.toString()),
            immutableEntry("char", SqlType.CHAR.toString()),
            immutableEntry("text", SqlType.VARCHAR.toString()),
            immutableEntry("date", SqlType.DATE.toString()),
            immutableEntry("time", SqlType.TIME.toString()),
            immutableEntry("timetz", SqlType.TIME.toString()),
            immutableEntry("timestamp", SqlType.TIMESTAMP.toString()),
            immutableEntry("timestamptz", SqlType.TIMESTAMP.toString()),
            immutableEntry("array", SqlType.ARRAY.toString()));

    public static ResultSet createTableResultSet(
            String schemaPattern, String tableNamePattern, String[] types, DataCloudStatement dataCloudStatement)
            throws SQLException {

        String tablesQuery = getTablesQuery(schemaPattern, tableNamePattern, types);
        ResultSet resultSet = dataCloudStatement.executeQuery(tablesQuery);
        List<Object> data = constructTableData(resultSet);
        QueryDBMetadata queryDbMetadata = QueryDBMetadata.GET_TABLES;

        return getMetadataResultSet(queryDbMetadata, NUM_TABLE_METADATA_COLUMNS, data);
    }

    static AvaticaResultSet getMetadataResultSet(QueryDBMetadata queryDbMetadata, int columnsCount, List<Object> data)
            throws SQLException {
        QueryResultSetMetadata queryResultSetMetadata = new QueryResultSetMetadata(queryDbMetadata);
        List<ColumnMetaData> columnMetaData =
                ArrowUtils.convertJDBCMetadataToAvaticaColumns(queryResultSetMetadata, columnsCount);
        Meta.Signature signature = new Meta.Signature(
                columnMetaData, null, Collections.emptyList(), Collections.emptyMap(), null, Meta.StatementType.SELECT);
        return MetadataResultSet.of(
                null, new QueryState(), signature, queryResultSetMetadata, TimeZone.getDefault(), null, data);
    }

    private static List<Object> constructTableData(ResultSet resultSet) throws SQLException {
        List<Object> data = new ArrayList<>();
        try {
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
        } catch (SQLException e) {
            throw new DataCloudJDBCException(e);
        }
        return data;
    }

    private static String getTablesQuery(String schemaPattern, String tableNamePattern, String[] types) {
        String tablesQuery = QueryResources.getTablesQuery();

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
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern,
            DataCloudStatement dataCloudStatement)
            throws SQLException {

        String getColumnsQuery = getColumnsQuery(schemaPattern, tableNamePattern, columnNamePattern);
        ResultSet resultSet = dataCloudStatement.executeQuery(getColumnsQuery);
        List<Object> data = constructColumnData(resultSet);
        QueryDBMetadata queryDbMetadata = QueryDBMetadata.GET_COLUMNS;

        return getMetadataResultSet(queryDbMetadata, NUM_COLUMN_METADATA_COLUMNS, data);
    }

    private static String getColumnsQuery(String schemaPattern, String tableNamePattern, String columnNamePattern) {
        String getColumnsQuery = QueryResources.getColumnsQuery();

        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            getColumnsQuery += " AND n.nspname LIKE " + quoteStringLiteral(schemaPattern);
        }
        if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
            getColumnsQuery += " AND c.relname LIKE " + quoteStringLiteral(tableNamePattern);
        }
        if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
            getColumnsQuery += " AND attname LIKE " + quoteStringLiteral(columnNamePattern);
        }
        getColumnsQuery += " ORDER BY nspname, c.relname, attnum ";

        return getColumnsQuery;
    }

    private static List<Object> constructColumnData(ResultSet resultSet) throws SQLException {
        List<Object> data = new ArrayList<>();
        try {
            while (resultSet.next()) {
                Object[] rowData = new Object[24];

                String tableCatalog = null;
                rowData[TABLE_CATALOG_INDEX] = tableCatalog;

                String tableSchema = resultSet.getString("nspname");
                rowData[TABLE_SCHEMA_INDEX] = tableSchema;

                String tableName = resultSet.getString("relname");
                rowData[TABLE_NAME_INDEX] = tableName;

                String columnName = resultSet.getString("attname");
                rowData[COLUMN_NAME_INDEX] = columnName;

                int dataType = (int) resultSet.getLong("atttypid");
                rowData[DATA_TYPE_INDEX] = dataType;

                String typeName = resultSet.getString("datatype");
                typeName = typeName == null ? StringUtils.EMPTY : typeName;
                if (typeName.toLowerCase().contains("numeric")) {
                    rowData[TYPE_NAME_INDEX] = SqlType.NUMERIC.toString();
                    rowData[DATA_TYPE_INDEX] = SqlType.valueOf(SqlType.NUMERIC.toString()).id;
                } else {
                    rowData[TYPE_NAME_INDEX] = dbTypeToSql.getOrDefault(typeName.toLowerCase(), typeName);
                    dataType = dbTypeToSql.containsKey(typeName.toLowerCase())
                            ? SqlType.valueOf(dbTypeToSql.get(typeName.toLowerCase())).id
                            : (int) resultSet.getLong("atttypid");
                    rowData[DATA_TYPE_INDEX] = dataType;
                }
                int columnSize = 255;
                rowData[COLUMN_SIZE_INDEX] = columnSize;

                int decimalDigits = 2;
                rowData[DECIMAL_DIGITS_INDEX] = decimalDigits;

                int numPrecRadix = 10;
                rowData[NUM_PREC_RADIX_INDEX] = numPrecRadix;

                int nullable = resultSet.getBoolean("attnotnull")
                        ? DatabaseMetaData.columnNoNulls
                        : DatabaseMetaData.columnNullable;
                rowData[NULLABLE_INDEX] = nullable;

                String description = resultSet.getString("description");
                rowData[DESCRIPTION_INDEX] = description;

                String columnDefault = resultSet.getString("adsrc");
                rowData[COLUMN_DEFAULT_INDEX] = columnDefault;

                rowData[SQL_DATA_TYPE_INDEX] = null;

                String sqlDateTimeSub = StringUtils.EMPTY;
                rowData[SQL_DATE_TIME_SUB_INDEX] = sqlDateTimeSub;

                int charOctetLength = 2;
                rowData[CHAR_OCTET_LENGTH_INDEX] = charOctetLength;

                int ordinalPosition = resultSet.getInt("attnum");
                rowData[ORDINAL_POSITION_INDEX] = ordinalPosition;

                String isNullable = resultSet.getBoolean("attnotnull") ? "NO" : "YES";
                rowData[IS_NULLABLE_INDEX] = isNullable;

                rowData[SCOPE_CATALOG_INDEX] = null;
                rowData[SCOPE_SCHEMA_INDEX] = null;
                rowData[SCOPE_TABLE_INDEX] = null;
                rowData[SOURCE_DATA_TYPE_INDEX] = null;

                String identity = resultSet.getString("attidentity");
                String defval = resultSet.getString("adsrc");
                String autoIncrement = "NO";
                if ((defval != null && defval.contains("nextval(")) || identity != null) {
                    autoIncrement = "YES";
                }
                rowData[AUTO_INCREMENT_INDEX] = autoIncrement;

                String generated = resultSet.getString("attgenerated");
                String generatedColumn = generated != null ? "YES" : "NO";
                rowData[GENERATED_COLUMN_INDEX] = generatedColumn;

                data.add(Arrays.asList(rowData));
            }
        } catch (SQLException e) {
            throw new DataCloudJDBCException(e);
        }
        return data;
    }

    public static ResultSet createSchemaResultSet(String schemaPattern, DataCloudStatement dataCloudStatement)
            throws SQLException {

        String schemasQuery = getSchemasQuery(schemaPattern);
        ResultSet resultSet = dataCloudStatement.executeQuery(schemasQuery);
        List<Object> data = constructSchemaData(resultSet);
        QueryDBMetadata queryDbMetadata = QueryDBMetadata.GET_SCHEMAS;

        return getMetadataResultSet(queryDbMetadata, NUM_SCHEMA_METADATA_COLUMNS, data);
    }

    private static String getSchemasQuery(String schemaPattern) {
        String schemasQuery = QueryResources.getSchemasQuery();
        if (StringUtils.isNotEmpty(schemaPattern)) {
            schemasQuery += " AND nspname LIKE " + quoteStringLiteral(schemaPattern);
        }
        return schemasQuery;
    }

    private static List<Object> constructSchemaData(ResultSet resultSet) throws SQLException {
        List<Object> data = new ArrayList<>();

        try {
            while (resultSet.next()) {
                List<Object> rowData =
                        Arrays.asList(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_CATALOG"));
                data.add(rowData);
            }
        } catch (SQLException e) {
            throw new DataCloudJDBCException(e);
        }
        return data;
    }

    public static ResultSet createTableTypesResultSet() throws SQLException {

        List<Object> data = constructTableTypesData();
        QueryDBMetadata queryDbMetadata = QueryDBMetadata.GET_TABLE_TYPES;

        return getMetadataResultSet(queryDbMetadata, NUM_TABLE_TYPES_METADATA_COLUMNS, data);
    }

    private static List<Object> constructTableTypesData() {
        List<Object> data = new ArrayList<>();

        for (Map.Entry<String, Map<String, String>> entry : tableTypeClauses.entrySet()) {
            List<Object> rowData = Arrays.asList(entry.getValue());
            data.add(rowData);
        }
        return data;
    }

    @SneakyThrows
    static List<Object> getLakehouse(Optional<TokenProcessor> tokenProcessor) {
        if (!tokenProcessor.isPresent()) {
            return ImmutableList.of();
        }

        val tenantId = tokenProcessor.get().getDataCloudToken().getTenantId();
        val dataspace = tokenProcessor.get().getSettings().getDataspace();
        val lakehouse =
                "lakehouse:" + tenantId + ";" + Optional.ofNullable(dataspace).orElse("");

        return ImmutableList.of(ImmutableList.of(lakehouse));
    }

    public static ResultSet createCatalogsResultSet(Optional<TokenProcessor> tokenProcessor) throws SQLException {
        val data = getLakehouse(tokenProcessor);
        val queryDbMetadata = QueryDBMetadata.GET_CATALOGS;

        return getMetadataResultSet(queryDbMetadata, NUM_CATALOG_METADATA_COLUMNS, data);
    }

    public static List<String> createDataspacesResponse(Optional<TokenProcessor> tokenProcessor, OkHttpClient client)
            throws SQLException {

        try {

            val dataspaceResponse = getDataSpaceResponse(tokenProcessor, client, false);
            return dataspaceResponse.getRecords().stream()
                    .map(DataspaceResponse.DataSpaceAttributes::getName)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new DataCloudJDBCException(e);
        }
    }

    private FormCommand buildGetDataspaceFormCommand(OAuthToken oAuthToken) throws URISyntaxException {
        val builder = FormCommand.builder();
        builder.url(oAuthToken.getInstanceUrl());
        builder.suffix(new URI(SOQL_ENDPOINT_SUFFIX));
        builder.queryParameters(ImmutableMap.of(SOQL_QUERY_PARAM_KEY, "SELECT+name+from+Dataspace"));
        builder.header(Constants.AUTHORIZATION, oAuthToken.getBearerToken());
        builder.header(FormCommand.CONTENT_TYPE_HEADER_NAME, Constants.CONTENT_TYPE_JSON);
        builder.header("User-Agent", "cdp/jdbc");
        builder.header("enable-stream-flow", "false");
        return builder.build();
    }

    private static DataspaceResponse getDataSpaceResponse(
            Optional<TokenProcessor> tokenProcessor, OkHttpClient client, boolean isGetCatalog) throws SQLException {
        String errorMessage = isGetCatalog
                ? "Token processor is empty. getCatalogs() cannot be executed"
                : "Token processor is empty. getDataspaces() cannot be executed";

        if (!tokenProcessor.isPresent()) {
            throw new DataCloudJDBCException(errorMessage);
        }
        try {
            val oAuthToken = tokenProcessor.get().getOAuthToken();
            FormCommand httpFormCommand = buildGetDataspaceFormCommand(oAuthToken);

            return FormCommand.get(client, httpFormCommand, DataspaceResponse.class);

        } catch (Exception e) {
            throw new DataCloudJDBCException(e);
        }
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
