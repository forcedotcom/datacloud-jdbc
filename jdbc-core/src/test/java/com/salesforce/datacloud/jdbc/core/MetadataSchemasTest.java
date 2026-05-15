/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.types.HyperTypes;
import com.salesforce.datacloud.jdbc.protocol.data.ColumnMetadata;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class MetadataSchemasTest {
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

    private static final List<String> TYPE_INFO_NAMES = Arrays.asList(
            "TYPE_NAME",
            "DATA_TYPE",
            "PRECISION",
            "LITERAL_PREFIX",
            "LITERAL_SUFFIX",
            "CREATE_PARAMS",
            "NULLABLE",
            "CASE_SENSITIVE",
            "SEARCHABLE",
            "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE",
            "AUTO_INCREMENT",
            "LOCAL_TYPE_NAME",
            "MINIMUM_SCALE",
            "MAXIMUM_SCALE",
            "SQL_DATA_TYPE",
            "SQL_DATETIME_SUB",
            "NUM_PREC_RADIX");

    private static final List<String> TYPE_INFO_TYPES = Arrays.asList(
            "TEXT", "INTEGER", "INTEGER", "TEXT", "TEXT", "TEXT", "SHORT", "BOOL", "SHORT", "BOOL", "BOOL", "BOOL",
            "TEXT", "SHORT", "SHORT", "INTEGER", "INTEGER", "INTEGER");

    private static final List<Integer> TYPE_INFO_TYPE_IDS = Arrays.asList(
            Types.VARCHAR,
            Types.INTEGER,
            Types.INTEGER,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.SMALLINT,
            Types.BOOLEAN,
            Types.SMALLINT,
            Types.BOOLEAN,
            Types.BOOLEAN,
            Types.BOOLEAN,
            Types.VARCHAR,
            Types.SMALLINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.INTEGER,
            Types.INTEGER);

    @Test
    void columnsSchemaHasExpectedNames() {
        List<String> names =
                MetadataSchemas.COLUMNS.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        assertThat(names).isEqualTo(COLUMN_NAMES);
        assertThat(names).hasSize(24);
        assertThat(names.get(0)).isEqualTo("TABLE_CAT");
    }

    @Test
    void columnsSchemaHasExpectedTypeNames() {
        List<String> typeNames = MetadataSchemas.COLUMNS.stream()
                .map(ColumnMetadata::getTypeName)
                .collect(Collectors.toList());
        assertThat(typeNames).isEqualTo(COLUMN_TYPES);
        assertThat(typeNames).hasSize(24);
        assertThat(typeNames.get(0)).isEqualTo("TEXT");
    }

    @Test
    void columnsSchemaHasExpectedJdbcTypeIds() {
        List<Integer> typeIds = MetadataSchemas.COLUMNS.stream()
                .map(c -> HyperTypes.toJdbcTypeCode(c.getType()))
                .collect(Collectors.toList());
        assertThat(typeIds).isEqualTo(COLUMN_TYPE_IDS);
        assertThat(typeIds).hasSize(24);
        assertThat(typeIds.get(0)).isEqualTo(Types.VARCHAR);
    }

    @Test
    void typeInfoSchemaHasExpectedNames() {
        List<String> names =
                MetadataSchemas.TYPE_INFO.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        assertThat(names).isEqualTo(TYPE_INFO_NAMES);
        assertThat(names).hasSize(18);
        assertThat(names.get(0)).isEqualTo("TYPE_NAME");
    }

    @Test
    void typeInfoSchemaHasExpectedTypeNames() {
        List<String> typeNames = MetadataSchemas.TYPE_INFO.stream()
                .map(ColumnMetadata::getTypeName)
                .collect(Collectors.toList());
        assertThat(typeNames).isEqualTo(TYPE_INFO_TYPES);
        assertThat(typeNames).hasSize(18);
        assertThat(typeNames.get(7)).isEqualTo("BOOL");
    }

    @Test
    void typeInfoSchemaHasExpectedJdbcTypeIds() {
        List<Integer> typeIds = MetadataSchemas.TYPE_INFO.stream()
                .map(c -> HyperTypes.toJdbcTypeCode(c.getType()))
                .collect(Collectors.toList());
        assertThat(typeIds).isEqualTo(TYPE_INFO_TYPE_IDS);
        assertThat(typeIds).hasSize(18);
        assertThat(typeIds.get(7)).isEqualTo(Types.BOOLEAN);
    }
}
