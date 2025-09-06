/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.util.Constants.INTEGER;
import static com.salesforce.datacloud.jdbc.util.Constants.SHORT;
import static com.salesforce.datacloud.jdbc.util.Constants.TEXT;

import com.google.common.collect.ImmutableList;
import java.sql.Types;
import java.util.List;

public enum QueryDBMetadata {
    GET_TABLE_TYPES(ImmutableList.of("TABLE_TYPE"), ImmutableList.of(TEXT), ImmutableList.of(Types.VARCHAR)),
    GET_CATALOGS(ImmutableList.of("TABLE_CAT"), ImmutableList.of(TEXT), ImmutableList.of(Types.VARCHAR)),
    GET_SCHEMAS(
            ImmutableList.of("TABLE_SCHEM", "TABLE_CATALOG"),
            ImmutableList.of(TEXT, TEXT),
            ImmutableList.of(Types.VARCHAR, Types.VARCHAR)),
    GET_TABLES(
            ImmutableList.of(
                    "TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    "REMARKS",
                    "TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "SELF_REFERENCING_COL_NAME",
                    "REF_GENERATION"),
            ImmutableList.of(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT),
            ImmutableList.of(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR)),
    GET_COLUMNS(
            ImmutableList.of(
                    "TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "COLUMN_SIZE",
                    "BUFFER_LENGTH",
                    "DECIMAL_DIGITS",
                    "NUM_PREC_RADIX",
                    "NULLABLE",
                    "REMARKS",
                    "COLUMN_DEF",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION",
                    "IS_NULLABLE",
                    "SCOPE_CATALOG",
                    "SCOPE_SCHEMA",
                    "SCOPE_TABLE",
                    "SOURCE_DATA_TYPE",
                    "IS_AUTOINCREMENT",
                    "IS_GENERATEDCOLUMN"),
            ImmutableList.of(
                    TEXT, TEXT, TEXT, TEXT, INTEGER, TEXT, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, TEXT, TEXT,
                    INTEGER, INTEGER, INTEGER, INTEGER, TEXT, TEXT, TEXT, TEXT, SHORT, TEXT, TEXT),
            ImmutableList.of(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR));

    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final List<Integer> columnTypeIds;

    QueryDBMetadata(List<String> columnNames, List<String> columnTypes, List<Integer> columnTypeIds) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnTypeIds = columnTypeIds;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public List<Integer> getColumnTypeIds() {
        return columnTypeIds;
    }
}
