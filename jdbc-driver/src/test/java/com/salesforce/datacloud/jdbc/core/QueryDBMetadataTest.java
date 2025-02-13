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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class QueryDBMetadataTest {
    private static final List<String> COLUMN_NAMES = Arrays.asList(
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
            "IS_GENERATEDCOLUMN");

    private static final List<String> COLUMN_TYPES = Arrays.asList(
            "TEXT", "TEXT", "TEXT", "TEXT", "INTEGER", "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "INTEGER",
            "TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "TEXT",
            "TEXT");

    private static final List<Integer> COLUMN_TYPE_IDS = Arrays.asList(
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
            Types.VARCHAR);

    @Test
    public void testGetColumnNames() {
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnNames()).isEqualTo(COLUMN_NAMES);
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnNames().size()).isEqualTo(24);
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnNames().get(0)).isEqualTo("TABLE_CAT");
    }

    @Test
    public void testGetColumnTypes() {
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnTypes()).isEqualTo(COLUMN_TYPES);
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnTypes().size()).isEqualTo(24);
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnTypes().get(0)).isEqualTo("TEXT");
    }

    @Test
    public void testGetColumnTypeIds() {
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnTypeIds()).isEqualTo(COLUMN_TYPE_IDS);
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnTypeIds().size()).isEqualTo(24);
        assertThat(QueryDBMetadata.GET_COLUMNS.getColumnTypeIds().get(0)).isEqualTo(Types.VARCHAR);
    }
}
