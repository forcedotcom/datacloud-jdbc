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

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.util.Constants;
import java.sql.Types;
import java.util.List;

public enum QueryDBMetadata {
    GET_TABLE_TYPES(ImmutableList.of("TABLE_TYPE"), ImmutableList.of(Constants.TEXT), ImmutableList.of(Types.VARCHAR)),
    GET_CATALOGS(ImmutableList.of("TABLE_CAT"), ImmutableList.of(Constants.TEXT), ImmutableList.of(Types.VARCHAR)),
    GET_SCHEMAS(
            ImmutableList.of("TABLE_SCHEM", "TABLE_CATALOG"),
            ImmutableList.of(Constants.TEXT, Constants.TEXT),
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
            ImmutableList.of(
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT),
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
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.INTEGER,
                    Constants.TEXT,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.INTEGER,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.TEXT,
                    Constants.SHORT,
                    Constants.TEXT,
                    Constants.TEXT),
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
