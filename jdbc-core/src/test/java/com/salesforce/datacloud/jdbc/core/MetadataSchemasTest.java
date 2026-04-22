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
}
